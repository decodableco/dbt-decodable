import re
import subprocess
import sys


def main():
    out = subprocess.run(["dbt", "--version"],  capture_output=True, text=True)
    print(out.stdout)
    plugin_detected = (
        re.search(r"Plugins:\s.*- decodable: \d+\.\d+\.\d+", out.stdout, flags=re.DOTALL)
        is not None
    )
    if not plugin_detected:
        sys.exit(
            f"Decodable plugin not recognized by dbt! Received output of `dbt --version`:\n{out.stdout}"
        )


if __name__ == "__main__":
    main()
