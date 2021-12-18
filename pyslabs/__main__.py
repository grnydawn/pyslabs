"""main entry for pyslabs command-line interface"""
  

def main():
    import argparse
    from pyslabs.info import version

    parser = argparse.ArgumentParser(description="pyslabs command-line tool")
    parser.add_argument("--version", action="version", version="pyslabs "+version)


    argps = parser.parse_args()

    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
