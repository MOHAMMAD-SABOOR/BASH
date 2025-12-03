#!/bin/bash
set -euo pipefail

# -----------------------------
# Configuration
# -----------------------------
DEFAULT_SHELL="/bin/bash"
HDFS_SUPERUSER="hdfs"

ok() { echo "[OK] $1"; }
err() { echo "[ERROR] $1" >&2; exit 1; }

# -----------------------------
# Root/Sudo check
# -----------------------------
[[ $EUID -ne 0 ]] && err "This script must be run as root or with sudo."
ok "Running with root privileges."

# -----------------------------
# Read inputs
# -----------------------------
read -rp "Enter username: " USERNAME
read -rp "Enter REALM (EXAMPLE.COM): " REALM
read -rp "Enter existing Linux group for the user: " USER_GROUP

PRINCIPAL="${USERNAME}@${REALM}"
HDFS_HOME="/user/$USERNAME"

# Strong Random password.
TEMP_PASSWORD=$(openssl rand -base64 12)

# -----------------------------
# Validations
# -----------------------------
id "$USERNAME" &>/dev/null && err "User already exists."
getent group "$USER_GROUP" >/dev/null || err "Group does not exist."

# -----------------------------
# Create POSIX user
# -----------------------------
useradd -m -g "$USER_GROUP" -e 0 "$USERNAME"
echo "$USERNAME:$TEMP_PASSWORD" | chpasswd
chage -d 0 "$USERNAME"  # Force password change at first login
ok "POSIX user created and set password will be forced to change at first login."

# -----------------------------
# Create Kerberos Principal
# -----------------------------
kadmin.local -q "getprinc $USERNAME" &>/dev/null && err "Kerberos principal already exists."
kadmin.local -q "addprinc -pw $TEMP_PASSWORD $USERNAME"
ok "Kerberos principal created."

# -----------------------------
# Create HDFS directory
# -----------------------------
sudo -u "$HDFS_SUPERUSER" hdfs dfs -mkdir -p "$HDFS_HOME"
sudo -u "$HDFS_SUPERUSER" hdfs dfs -chown "$USERNAME:$USER_GROUP" "$HDFS_HOME"
sudo -u "$HDFS_SUPERUSER" hdfs dfs -chmod 700 "$HDFS_HOME"
ok "HDFS home directory created and permissions are applied."

# -----------------------------
# Final Summary
# -----------------------------
echo "======================================="
echo "User onboarding completed successfully."
echo "USERNAME       : $USERNAME"
echo "KERBEROS ID    : $PRINCIPAL"
echo "TEMP PASSWORD  : $TEMP_PASSWORD"
echo "(User must change password at first login)"
echo "======================================="
