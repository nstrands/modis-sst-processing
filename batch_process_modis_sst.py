import xarray as xr
import rioxarray
import numpy as np
import os
import sys

# === CONFIG ===
INPUT_DIR = "Data/MODIS"
OUTPUT_DIR = "output"
SST_VAR_NAME = "sst"  # expected variable name
NODATA_VALUE = -9999.0
OUTPUT_SUFFIX = "_SST.tif"

def process_sst_file(nc_path, output_path):
    try:
        ds = xr.open_dataset(nc_path)
        if SST_VAR_NAME not in ds.variables:
            print(f"⚠️ Skipped {os.path.basename(nc_path)} — variable '{SST_VAR_NAME}' not found.")
            return

        sst = ds[SST_VAR_NAME]
        attrs = sst.attrs

        scale_factor = attrs.get("scale_factor", 1.0)
        add_offset = attrs.get("add_offset", 0.0)
        fill_value = attrs.get("_FillValue", 0)

        # Apply scale and offset, then mask
        scaled_sst = sst * scale_factor + add_offset
        cleaned_sst = scaled_sst.where(sst != fill_value, NODATA_VALUE)

        # Set spatial reference
        cleaned_sst.rio.set_spatial_dims(x_dim="lon", y_dim="lat", inplace=True)
        cleaned_sst.rio.write_crs("EPSG:4326", inplace=True)
        cleaned_sst.rio.write_nodata(NODATA_VALUE, inplace=True)

        # Save GeoTIFF
        cleaned_sst.rio.to_raster(output_path)
        print(f"✅ Processed: {os.path.basename(nc_path)} → {os.path.basename(output_path)}")

    except Exception as e:
        print(f"❌ Error processing {os.path.basename(nc_path)}: {e}")

def batch_process_all_nc_files(input_dir, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    nc_files = [f for f in os.listdir(input_dir) if f.endswith(".nc")]
    print(f"\n🔍 Found {len(nc_files)} .nc files to process...\n")

    for filename in sorted(nc_files):
        nc_path = os.path.join(input_dir, filename)
        base_name = filename.replace(".L3m.DAY.SST.x_sst.nc", "")
        output_path = os.path.join(output_dir, base_name + OUTPUT_SUFFIX)

        process_sst_file(nc_path, output_path)

if __name__ == "__main__":
    batch_process_all_nc_files(INPUT_DIR, OUTPUT_DIR)
