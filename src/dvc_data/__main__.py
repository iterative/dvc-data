try:
    from .cli import main
except ImportError:  # pragma: no cover

    def main():  # type: ignore[misc]
        import sys

        print(
            "dvc-data could not run because the required "
            "dependencies are not installed.\n"
            "Please install it with: pip install 'dvc-data[cli]'"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
