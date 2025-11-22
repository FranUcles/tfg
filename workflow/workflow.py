#!/usr/bin/python3
import logging
import coloredlogs
import argparse
import subprocess
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent

# Dict struct: "step_name": "command_details"
# Each "command_details" is a duple containing: (python env, [command, arguments...])

STEPS = {
    "parse_umap": ("tfg_visuals", ["python", str(BASE_DIR.parent) + "/tools/ndfield_parser.py"]),
    "delaunay": ("disperse", ["delaunay_3D"])
}

def parse_args():
    parser = argparse.ArgumentParser(description="Process cloud points")
    parser.add_argument("-d", "--debug", action="store_true", help="Enable debug logs")
    parser.add_argument("-q", "--quiet", action="store_true", help="Show only errors")
    parser.add_argument("--no-logs", action="store_true", help="Disable all logs")
    parser.add_argument("-i", "--input", type=str, required=True, help="Input CSV filename")
    parser.add_argument("-o", "--output", type=str, default=None, help="Output VTP filename")
    return parser.parse_args()

def configure_logging(args):
    if args.no_logs:
        # Disable all logs, even CRITICAL
        logging.disable(logging.CRITICAL)
        return ["--no-logs"]
    logs_arg = []
    if args.quiet:
        level = logging.ERROR
        logs_arg = ["-q"]
    elif args.debug:
        level = logging.DEBUG
        logs_arg = ["-d"]
    else:
        level = logging.INFO

    coloredlogs.install(
        level=level,
        logger=logger,
        fmt='%(asctime)s [%(levelname)s] : %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logs_arg

def run_step(step, args):
    env, cmd = STEPS[step]
    logger.debug(f"Starting {step}...")
    subprocess.run(["mamba", "run", "-n", env] + cmd + args, check=True)
    logger.debug(f"{step} finished!")

def main():
    args = parse_args()
    logs_arguments = configure_logging(args)
    logger.info("Starting workflow")
    with tempfile.NamedTemporaryFile() as t1:
        # Convert to ndfield
        run_step("parse_umap", logs_arguments + ["-i", args.input, "-o", t1.name])
        # Apply delaunay
        run_step("delaunay", [t1.name, "-outName", args.output])

if __name__ == "__main__":
    main()

