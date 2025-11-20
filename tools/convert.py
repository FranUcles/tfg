#!/usr/bin/python3
import logging
import vtk
import argparse

logger = logging.getLogger(__name__)

def save_vtp(poly, fname):
    """
    Store data vtkPolyData as a .vtp file

    :param poly: input vtkPolyData to store
    :param fname: output path file
    :return:
    """
    logger.info("Saving the polygon to a file...")
    writer = vtk.vtkXMLPolyDataWriter()
    writer.SetFileName(fname)
    writer.SetInputData(poly)
    if writer.Write() != 1:
        logger.error(f"Unable to write polygon to file {fname}")
        raise IOError(f"Unable to write polygon to file {fname}")
    logger.info("Polygon saved successfully!")


def cloud_point_to_poly(cmass):
    logger.info("Converting the cloud point to a polygon...")
    # Initialization
    poly = vtk.vtkPolyData()
    points = vtk.vtkPoints()
    verts = vtk.vtkCellArray()
    # Applying mask
    count = 0
    for cmas in cmass:
        x, y, z = cmas[0], cmas[1], cmas[2]
        # Insert the center of mass in the poly data
        points.InsertNextPoint((x, y, z))
        verts.InsertNextCell(1)
        verts.InsertCellPoint(count)
        count += 1
    logger.debug(f"Total of points: {count}")
    # Creates the poly
    poly.SetPoints(points)
    poly.SetVerts(verts)
    logger.info("Points converted successfully to a polygon!")
    return poly


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
        return

    if args.quiet:
        level = logging.ERROR
    elif args.debug:
        level = logging.DEBUG
    else:
        level = logging.INFO

    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    )

def read_points_from_file(fname):
    # TODO Read the cloud point
    pass

def main():
    args = parse_args()
    configure_logging(args)

    logger.info("Starting cloud point processing...")

    cmass = read_points_from_file(args.input)

    if not cmass:
        logger.error("No points to process. Exiting.")
        return

    poly = cloud_point_to_poly(cmass)

    if args.output:
        save_vtp(poly, args.output)
    else:
        logger.info("No output file provided; skipping save.")

    logger.info("Processing completed.")

if __name__ == "__main__":
    main()
