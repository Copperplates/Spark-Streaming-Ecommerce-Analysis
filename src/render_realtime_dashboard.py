import argparse

from realtime_reporting import render_dashboard


def parse_args():
    parser = argparse.ArgumentParser(description="Regenerate realtime dashboard files.")
    parser.add_argument(
        "--output-dir",
        default="/opt/spark-results/realtime",
        help="Directory that contains realtime CSV outputs.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    render_dashboard(args.output_dir)
    print(f"Realtime dashboard regenerated under {args.output_dir}.")


if __name__ == "__main__":
    main()
