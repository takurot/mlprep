import argparse
import sys
import mlprep

def main():
    parser = argparse.ArgumentParser(description="mlprep CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # run command
    run_parser = subparsers.add_parser("run", help="Run a pipeline")
    run_parser.add_argument("pipeline", help="Path to pipeline.yaml")

    args = parser.parse_args()

    if args.command == "run":
        try:
            mlprep.run_pipeline(args.pipeline)
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)

if __name__ == "__main__":
    main()
