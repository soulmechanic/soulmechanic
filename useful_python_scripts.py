# Python scripts for certain requirement

# Pythonic/efficient way to strip whitespace from every Pandas Data frame cell that has a stringlike object in it

data_frame_trimmed = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)