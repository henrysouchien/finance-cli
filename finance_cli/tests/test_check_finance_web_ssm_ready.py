from __future__ import annotations

import os
import subprocess
import textwrap
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "check_finance_web_ssm_ready.sh"


def _write_fake_aws(tmp_path: Path) -> Path:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_aws = fake_bin / "aws"
    fake_aws.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env python3
            from __future__ import annotations

            import os
            import sys
            from pathlib import Path


            args = sys.argv[1:]
            service = args[0] if len(args) > 0 else ""
            operation = args[1] if len(args) > 1 else ""

            if service == "ec2" and operation == "describe-instances":
                print("running")
            elif service == "ec2" and operation == "describe-iam-instance-profile-associations":
                print("arn:aws:iam::948633118115:instance-profile/finance-web-ec2-profile")
            elif service == "iam" and operation == "get-instance-profile":
                print("finance-web-ec2-role")
            elif service == "iam" and operation == "list-attached-role-policies":
                print("arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore")
            elif service == "ssm" and operation == "describe-instance-information":
                print("Online")
            elif service == "ssm" and operation == "send-command":
                print("cmd-123")
            elif service == "ssm" and operation == "get-command-invocation":
                mode = os.environ.get("FAKE_AWS_GET_INVOCATION", "success")
                if mode == "access_denied":
                    print(
                        "An error occurred (AccessDeniedException) when calling the "
                        "GetCommandInvocation operation: denied",
                        file=sys.stderr,
                    )
                    raise SystemExit(254)
                if mode == "missing_then_success":
                    state_path = Path(os.environ["FAKE_AWS_STATE"])
                    count = int(state_path.read_text() or "0") if state_path.exists() else 0
                    state_path.write_text(str(count + 1))
                    if count == 0:
                        print(
                            "An error occurred (InvocationDoesNotExist) when calling the "
                            "GetCommandInvocation operation: not found",
                            file=sys.stderr,
                        )
                        raise SystemExit(254)
                print("Success")
            else:
                print(f"unexpected aws invocation: {' '.join(args)}", file=sys.stderr)
                raise SystemExit(99)
            """
        )
    )
    fake_aws.chmod(0o755)
    return fake_bin


def _run_preflight(tmp_path: Path, mode: str) -> subprocess.CompletedProcess[str]:
    fake_bin = _write_fake_aws(tmp_path)
    env = os.environ.copy()
    env.update(
        {
            "FAKE_AWS_GET_INVOCATION": mode,
            "FAKE_AWS_STATE": str(tmp_path / "aws-state"),
            "PATH": f"{fake_bin}:{env.get('PATH', '')}",
        }
    )
    return subprocess.run(
        ["bash", str(SCRIPT), "--timeout-seconds", "5"],
        check=False,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=10,
    )


def test_ssm_preflight_surfaces_get_command_invocation_access_denied(
    tmp_path: Path,
) -> None:
    result = _run_preflight(tmp_path, "access_denied")

    assert result.returncode == 1
    assert "AccessDeniedException" in result.stderr
    assert "unable to get SSM command cmd-123 invocation status" in result.stderr
    assert "timed out" not in result.stderr


def test_ssm_preflight_tolerates_initial_invocation_not_visible(
    tmp_path: Path,
) -> None:
    result = _run_preflight(tmp_path, "missing_then_success")

    assert result.returncode == 0
    assert "PASS: finance-web SSM Run Command succeeded." in result.stdout
