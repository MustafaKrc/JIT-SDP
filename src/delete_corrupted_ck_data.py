from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
DATASET_FOLDER = ROOT_DIR / 'dataset' / 'java' / 'elasticsearch' / 'CK'


for file_path in DATASET_FOLDER.glob('*'):
    if file_path.is_file():
        with open(file_path, 'r') as file:
            content = file.read()
            if len(content) < 10:
                print(f'Deleting {file_path}')
                file_path.unlink()