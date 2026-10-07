#!/usr/bin/env bash
set -euo pipefail

instance_id="${1:?EC2 instance ID is required}"
tag="${2:?release tag is required}"
repository="${GITHUB_REPOSITORY:?GitHub repository is required}"
archive="firmament-x86_64-unknown-linux-gnu.tar.gz"
archive_url="https://github.com/$repository/releases/download/$tag/$archive"
checksum_url="$archive_url.sha256"
script_base64="$(base64 < deploy/deploy-release.sh | tr -d '\n')"
printf -v quoted_tag '%q' "$tag"
printf -v quoted_archive '%q' "$archive_url"
printf -v quoted_checksum '%q' "$checksum_url"
parameters="$(node -e 'const [install,run]=process.argv.slice(1); process.stdout.write(JSON.stringify({commands:[install,run]}))' "printf '%s' '$script_base64' | base64 -d > /tmp/firmament-deploy.sh" "bash /tmp/firmament-deploy.sh $quoted_tag $quoted_archive $quoted_checksum")"
command_id="$(aws ssm send-command --instance-ids "$instance_id" --document-name AWS-RunShellScript --comment "Deploy Firmament $tag" --parameters "$parameters" --query 'Command.CommandId' --output text)"
aws ssm wait command-executed --command-id "$command_id" --instance-id "$instance_id"
aws ssm get-command-invocation --command-id "$command_id" --instance-id "$instance_id" --query '{Status:Status,StandardOutput:StandardOutputContent,StandardError:StandardErrorContent}'
