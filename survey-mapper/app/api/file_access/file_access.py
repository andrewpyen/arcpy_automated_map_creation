import tempfile
from pathlib import Path as FSPath
import zipfile
import os
from pathlib import Path as FSPath
from fastapi import UploadFile

async def save_upload_to_temp_excel(upload: UploadFile) -> FSPath:
    """ Saves an uploaded Excel file to a temporary directory and returns the path.
        The file is saved with its original extension or .xlsx if no extension is provided.
    """
    suffix = FSPath(upload.filename).suffix or ".xlsx"  # type: ignore
    temp_dir = FSPath(tempfile.mkdtemp(prefix="excel_upload_"))
    temp_path = temp_dir / f"uploaded_excel{suffix}"
    contents = await upload.read()
    with open(temp_path, "wb") as f:
        f.write(contents)
    return temp_path


def zip_directory(src_dir: str, dest_zip: FSPath) -> FSPath:
    """ Zips the contents of src_dir into a zip file at dest_zip.
        Creates parent directories if they do not exist."""
    
    excluded_shp_files = [
        'NullRiser.json',
        'InactiveRiser.json'
    ]

    # # Assebmle a list of all filenames that end with '.lpkx' in the source directory
    # annotation_layer_names = []
    # for root, dirs, files in os.walk(src_dir):
    #     for name in files:
    #         if name.endswith('.lpkx'):
    #             annotation_layer_names.append(name)

    include_exts = ('.lpkx', '.json', '.geodatabase', '.csv', '.txt')  # only these

    dest_zip.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(dest_zip, "w", compression=zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
        for root, dirs, files in os.walk(src_dir):
            for name in files:
                # Exclude files in excluded_files list
                if name in excluded_shp_files:
                    continue
                # include only .lpkx, .json, .geodatabase
                if not name.lower().endswith(include_exts):
                    continue
                abs_path = FSPath(root) / name
                rel_path = os.path.relpath(abs_path, src_dir)
                print(f"Zipping rel path: {rel_path} and abs path: {abs_path}")
                zf.write(abs_path, arcname=str(rel_path))
    return dest_zip