import lzma
import struct
import pandas as pd


def bi5_to_df(filename, fmt):
    chunk_size = struct.calcsize(fmt)
    data = []
    with lzma.open(filename) as f:
        while True:
            chunk = f.read(chunk_size)
            if chunk:
                data.append(struct.unpack(fmt, chunk))
            else:
                break
    df = pd.DataFrame(data)
    df.to_csv(filename + ".csv")
    return df


if __name__ == "__main__":
    import sys
    bi5_to_df(sys.argv[1], '>3I2f')
