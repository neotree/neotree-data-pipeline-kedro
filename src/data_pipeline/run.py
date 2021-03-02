
"""Entry point for running a Kedro pipeline as a Python package."""
from pathlib import Path

from kedro.framework.session import KedroSession



def run_package():
    # Entry point for running a Kedro project packaged with `kedro package`
    # using `python -m <project_package>.run` command.
    package_name = Path(__file__).resolve().parent.name
    with KedroSession.create(package_name) as session:
            session.run()

if __name__ == "__main__":
    run_package()
