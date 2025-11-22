import pandas as pd
import numpy as np
import struct
import argparse
import logging
import coloredlogs

logger = logging.getLogger(__name__)

def parse_args():
    parser = argparse.ArgumentParser(description="Process cloud points")
    parser.add_argument("-d", "--debug", action="store_true", help="Enable debug logs")
    parser.add_argument("-q", "--quiet", action="store_true", help="Show only errors")
    parser.add_argument("--no-logs", action="store_true", help="Disable all logs")
    parser.add_argument("-i", "--input", type=str, required=True, help="Input Pandas DataFrame filename")
    parser.add_argument("-o", "--output", type=str, required=True, help="Output NDField filename")
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

    coloredlogs.install(
        level=level,
        logger=logger,
        fmt='%(asctime)s [%(levelname)s] : %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def parse_dataframe(fname, ndfield_file, comment="Python NDfield"):
    """
    Lee un DataFrame con columnas X,Y,Z y genera un NDfield binario compatible con C.
    """
    # --- Leer CSV ---
    df = pd.read_pickle(fname)
    cols= ['umap_0', 'umap_1', 'umap_2']
    if not all(c in df.columns for c in cols):
        logger.error(f"DataFrame must contain columns {cols}")
        raise ValueError(f"DataFrame must contain columns {cols}")

    points = df[cols].to_numpy(dtype=np.float64)
    npoints, ndims = points.shape

    # --- Parámetros NDfield ---
    fdims_index = 1      # nube de partículas
    datatype = 512       # ND_DOUBLE
    datasize = 8

    # --- Dims y padding ---
    NDFIELD_MAX_DIMS = 20
    dims = [0]*NDFIELD_MAX_DIMS
    dims[0] = ndims
    dims[1] = npoints
    x0 = [0.0]*NDFIELD_MAX_DIMS
    delta = [1.0]*NDFIELD_MAX_DIMS
    dummy = bytes(160)

    # --- Flatten data ---
    data_flat = points.ravel()

    # --- Calcular tamaño bloque header como en C ---
    header_block_size = (NDFIELD_MAX_DIMS + 3)*4 + 2*NDFIELD_MAX_DIMS*8 + 160 + 80

    # --- Escribir binario ---
    with open(ndfield_file, 'wb') as f:
        # TAG block
        f.write(struct.pack("i", 16))
        tag = "NDFIELD".ljust(16, "\x00")
        f.write(tag.encode("ascii"))
        f.write(struct.pack("i", 16))

        # Header block
        f.write(struct.pack("i", header_block_size))
        # comment[80]
        comment_bytes = comment.encode("ascii")[:80].ljust(80, b'\x00')
        f.write(comment_bytes)
        # ndims
        f.write(struct.pack("i", ndims))
        # dims[ndims] + padding
        f.write(struct.pack(f"{ndims}i", *dims[:ndims]))
        f.write(struct.pack(f"{NDFIELD_MAX_DIMS-ndims}i", *dims[ndims:]))
        # fdims_index
        f.write(struct.pack("i", fdims_index))
        # datatype
        f.write(struct.pack("i", datatype))
        # x0[ndims] + padding
        f.write(struct.pack(f"{ndims}d", *x0[:ndims]))
        f.write(struct.pack(f"{NDFIELD_MAX_DIMS-ndims}d", *x0[ndims:]))
        # delta[ndims] + padding
        f.write(struct.pack(f"{ndims}d", *delta[:ndims]))
        f.write(struct.pack(f"{NDFIELD_MAX_DIMS-ndims}d", *delta[ndims:]))
        # dummy[160]
        f.write(dummy)
        # cerrar header block
        f.write(struct.pack("i", header_block_size))

        # Data block
        data_bytes = data_flat.tobytes()
        f.write(struct.pack("i", len(data_bytes)))
        f.write(data_bytes)
        f.write(struct.pack("i", len(data_bytes)))

    logger.debug(f"NDfield saved on: {ndfield_file}, number of points: {npoints}, dimensions: {ndims}")

def main():
    args = parse_args()
    configure_logging(args)

    logger.info("Parsing the dataframe...")
    parse_dataframe(args.input, args.output)
    logger.info("Processing completed!")


if __name__ == "__main__":
    main()
