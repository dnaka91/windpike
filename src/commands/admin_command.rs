#![allow(dead_code)]

use std::str;

use super::{buffer::Buffer, CommandError, Result};
use crate::{
    cluster::Cluster,
    msgpack::Write,
    net::{Connection, PooledConnection},
    ResultCode,
};

#[derive(Clone, Copy)]
enum Command {
    Authenticate,
    CreateUser,
    DropUser,
    SetPassword,
    ChangePassword,
    GrantRoles,
    RevokeRoles,
    ReplaceRoles,
    QueryUsers = 9,
    Login = 20,
}

#[derive(Clone, Copy)]
enum FieldId {
    User,
    Password,
    OldPassword,
    Credential,
    Roles = 10,
}

// Misc
const MSG_VERSION: u64 = 2;
const MSG_TYPE: u64 = 2;

const HEADER_SIZE: usize = 24;
const HEADER_REMAINING: usize = HEADER_SIZE - std::mem::size_of::<i64>();

pub struct AdminCommand {}

impl AdminCommand {
    #[must_use]
    pub const fn new() -> Self {
        Self {}
    }

    async fn execute(mut conn: PooledConnection<'_>) -> Result<()> {
        // Send command.
        if let Err(err) = conn.flush().await {
            conn.close().await;
            return Err(err.into());
        }

        // read header
        if let Err(err) = conn.read_buffer(HEADER_SIZE).await {
            conn.close().await;
            return Err(err.into());
        }

        let buf = conn.buffer();
        buf.advance(9);
        let result_code = ResultCode::from(buf.read_u8());

        if result_code != ResultCode::Ok {
            return Err(CommandError::ServerError(result_code));
        }

        Ok(())
    }

    pub async fn authenticate(conn: &mut Connection, user: &str, password: &str) -> Result<()> {
        let buf = conn.buffer();
        buf.clear(1024)?;
        write_size(
            buf,
            HEADER_SIZE + estimate_field_size(user) + estimate_field_size(password),
        );
        write_header(buf, Command::Login, 2);
        write_field_str(buf, FieldId::User, user);
        write_field_bytes(buf, FieldId::Credential, password);

        conn.flush().await?;
        conn.read_buffer(HEADER_SIZE).await?;

        let buf = conn.buffer();
        let size = buf.read_u64();
        let size = (size & 0xffff_ffff_ffff) - HEADER_REMAINING as u64;

        buf.advance(1);
        let result_code = ResultCode::from(buf.read_u8());

        if ResultCode::SecurityNotEnabled != result_code && ResultCode::Ok != result_code {
            return Err(CommandError::ServerError(result_code));
        }

        // consume the rest of the buffer
        buf.advance(HEADER_REMAINING - 2);
        conn.read_buffer(size as usize).await?;

        Ok(())
    }

    pub async fn create_user(
        cluster: &Cluster,
        user: &str,
        password: &str,
        roles: &[&str],
    ) -> Result<()> {
        let password = hash_password(password)?;

        let node = cluster
            .get_random_node()
            .await
            .ok_or(CommandError::NoConnection)?;
        let mut conn = node.get_connection().await?;

        let buf = conn.buffer();
        buf.clear(1024)?;
        write_size(
            buf,
            HEADER_SIZE
                + estimate_field_size(user)
                + estimate_field_size(&password)
                + estimate_roles_size(roles),
        );
        write_header(buf, Command::CreateUser, 3);
        write_field_str(buf, FieldId::User, user);
        write_field_str(buf, FieldId::Password, password);
        write_roles(buf, roles);

        Self::execute(conn).await
    }

    pub async fn drop_user(cluster: &Cluster, user: &str) -> Result<()> {
        let node = cluster
            .get_random_node()
            .await
            .ok_or(CommandError::NoConnection)?;
        let mut conn = node.get_connection().await?;

        let buf = conn.buffer();
        buf.clear(1024)?;
        write_size(buf, HEADER_SIZE + estimate_field_size(user));
        write_header(buf, Command::DropUser, 1);
        write_field_str(buf, FieldId::User, user);

        Self::execute(conn).await
    }

    pub async fn set_password(cluster: &Cluster, user: &str, password: &str) -> Result<()> {
        let password = hash_password(password)?;

        let node = cluster
            .get_random_node()
            .await
            .ok_or(CommandError::NoConnection)?;
        let mut conn = node.get_connection().await?;

        let buf = conn.buffer();
        buf.clear(1024)?;
        write_size(
            buf,
            HEADER_SIZE + estimate_field_size(user) + estimate_field_size(&password),
        );
        write_header(buf, Command::SetPassword, 2);
        write_field_str(buf, FieldId::User, user);
        write_field_str(buf, FieldId::Password, password);

        Self::execute(conn).await
    }

    pub async fn change_password(cluster: &Cluster, user: &str, password: &str) -> Result<()> {
        let old_password = cluster
            .client_policy()
            .user_password
            .as_ref()
            .map(|(_, password)| hash_password(password))
            .transpose()?
            .unwrap_or_default();
        let password = hash_password(password)?;

        let node = cluster
            .get_random_node()
            .await
            .ok_or(CommandError::NoConnection)?;
        let mut conn = node.get_connection().await?;

        let buf = conn.buffer();
        buf.clear(1024)?;
        write_size(
            buf,
            HEADER_SIZE
                + estimate_field_size(user)
                + estimate_field_size(&old_password)
                + estimate_field_size(&password),
        );
        write_header(buf, Command::ChangePassword, 3);
        write_field_str(buf, FieldId::User, user);
        write_field_str(buf, FieldId::OldPassword, old_password);
        write_field_str(buf, FieldId::Password, password);

        Self::execute(conn).await
    }

    pub async fn grant_roles(cluster: &Cluster, user: &str, roles: &[&str]) -> Result<()> {
        let node = cluster
            .get_random_node()
            .await
            .ok_or(CommandError::NoConnection)?;
        let mut conn = node.get_connection().await?;

        let buf = conn.buffer();
        buf.clear(1024)?;
        write_size(
            buf,
            HEADER_SIZE + estimate_field_size(user) + estimate_roles_size(roles),
        );
        write_header(buf, Command::GrantRoles, 2);
        write_field_str(buf, FieldId::User, user);
        write_roles(buf, roles);

        Self::execute(conn).await
    }

    pub async fn revoke_roles(cluster: &Cluster, user: &str, roles: &[&str]) -> Result<()> {
        let node = cluster
            .get_random_node()
            .await
            .ok_or(CommandError::NoConnection)?;
        let mut conn = node.get_connection().await?;

        let buf = conn.buffer();
        buf.clear(1024)?;
        write_size(
            buf,
            HEADER_SIZE + estimate_field_size(user) + estimate_roles_size(roles),
        );
        write_header(buf, Command::RevokeRoles, 2);
        write_field_str(buf, FieldId::User, user);
        write_roles(buf, roles);

        Self::execute(conn).await
    }
}

fn write_size(buf: &mut Buffer, size: usize) {
    let size = (size as u64 - 8) | (MSG_VERSION << 56) | (MSG_TYPE << 48);
    buf.write_u64(size);
}

fn write_header(buf: &mut Buffer, command: Command, field_count: u8) {
    buf.write_u8(0);
    buf.write_u8(0);
    buf.write_u8(command as u8);
    buf.write_u8(field_count);
    buf.write_bytes(&[0; 12]);
}

const FIELD_HEADER_SIZE: usize = 5;

fn write_field_header(buf: &mut Buffer, id: FieldId, size: usize) {
    buf.write_u32(size as u32 + 1);
    buf.write_u8(id as u8);
}

fn estimate_field_size(s: impl AsRef<[u8]>) -> usize {
    FIELD_HEADER_SIZE + s.as_ref().len()
}

fn write_field_str(buf: &mut Buffer, id: FieldId, s: impl AsRef<str>) {
    let s = s.as_ref();
    write_field_header(buf, id, s.len());
    buf.write_str(s);
}

fn write_field_bytes(buf: &mut Buffer, id: FieldId, b: impl AsRef<[u8]>) {
    let b = b.as_ref();
    write_field_header(buf, id, b.len());
    buf.write_bytes(b);
}

fn estimate_roles_size(roles: &[&str]) -> usize {
    FIELD_HEADER_SIZE + roles.iter().map(|role| 1 + role.len()).sum::<usize>()
}

fn write_roles(buf: &mut Buffer, roles: &[&str]) {
    let mut size = 0;
    for role in roles {
        size += role.len() + 1; // size + len
    }

    write_field_header(buf, FieldId::Roles, size);
    buf.write_u8(roles.len() as u8);
    for role in roles {
        buf.write_u8(role.len() as u8);
        buf.write_str(role);
    }
}

pub fn hash_password(password: &str) -> Result<String> {
    const COST: u32 = 10;
    const SALT: [u8; 16] = [
        0xf4, 0x6b, 0x0b, 0xbe, 0xcf, 0xfe, 0x8d, 0x1b, 0x06, 0x67, 0xd8, 0x4f, 0x6d, 0xc1, 0xd8,
        0xa9,
    ];
    const VERSION: bcrypt::Version = bcrypt::Version::TwoA;

    Ok(bcrypt::hash_with_salt(password, COST, SALT)?.format_for_version(VERSION))
}
