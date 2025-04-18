import xarray as xr
import rioxarray
import numpy as np
import os
import sys

# === CONFIG ===
INPUT_FILE = "Data/MODIS/AQUA_MODIS.20230401.L3m.DAY.SST.x_sst.nc"
OUTPUT_DIR = "output"
SST_VAR_NAME = "sst"  # expected variable name

# === PROCESS ===
def process_sst_nc_file(nc_path, output_dir):
    ds = xr.open_dataset(nc_path)
    print(f"\nOpened: {nc_path}")

    # Confirm the variable exists
    if SST_VAR_NAME not in ds.variables:
        print(f"❗ Variable '{SST_VAR_NAME}' not found. Available variables:")
        for var in ds.data_vars:
            print(f" - {var}")
        sys.exit("Please update SST_VAR_NAME and try again.")

    sst = ds[SST_VAR_NAME]

    # Read metadata attributes
    attrs = sst.attrs
    print("\nSST variable attributes:")
    for key, value in attrs.items():
        print(f" - {key}: {value}")

    scale_factor = attrs.get("scale_factor", 1.0)
    add_offset = attrs.get("add_offset", 0.0)
    fill_value = attrs.get("_FillValue", 0)

    print(f"\nUsing scale_factor={scale_factor}, add_offset={add_offset}, fill_value={fill_value}")

    # Apply scale and offset
    scaled_sst = sst * scale_factor + add_offset

    # Replace fill values with explicit NoData value
    NODATA_VALUE = -9999.0
    cleaned_sst = scaled_sst.where(sst != fill_value, NODATA_VALUE)

    # Attach spatial reference
    try:
        cleaned_sst.rio.set_spatial_dims(x_dim="lon", y_dim="lat", inplace=True)
    except Exception as e:
        print(f"❗ Could not set spatial dimensions: {e}")
        sys.exit()

    cleaned_sst.rio.write_crs("EPSG:4326", inplace=True)
    cleaned_sst.rio.write_nodata(NODATA_VALUE, inplace=True)


    # Build output path
    base_name = os.path.basename(nc_path).replace(".L3m.DAY.SST.x_sst.nc", "")
    output_path = os.path.join(output_dir, f"{base_name}_SST.tif")

    # Create output directory if needed
    os.makedirs(output_dir, exist_ok=True)

    # Export
    cleaned_sst.rio.to_raster(output_path)
    print(f"\n✅ Saved: {output_path}")

if __name__ == "__main__":
    process_sst_nc_file(INPUT_FILE, OUTPUT_DIR)
