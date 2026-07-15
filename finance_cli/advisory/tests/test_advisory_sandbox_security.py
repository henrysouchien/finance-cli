from __future__ import annotations

from textwrap import dedent


def test_advisory_sandbox_network_egress_is_blocked(run_advisory_container) -> None:
    script = dedent(
        """
        import urllib.request

        try:
            urllib.request.urlopen("https://example.com", timeout=3)
        except Exception as exc:
            print(type(exc).__name__)
        else:
            raise SystemExit("network unexpectedly reachable")
        """
    )

    result = run_advisory_container(
        script,
        extra_args=[
            "--network",
            "none",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges",
            "--read-only",
            "--tmpfs",
            "/tmp:rw,noexec,nosuid,size=64m",
        ],
    )

    assert result.returncode == 0, result.stderr or result.stdout
    assert result.stdout.strip()
