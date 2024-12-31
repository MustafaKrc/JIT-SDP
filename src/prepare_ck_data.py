import os
import subprocess
import shutil
from pathlib import Path
from typing import List
import pandas as pd
import threading
import numpy as np
from git import Repo, GitCommandError
import datetime
import logging

# Constants
ROOT_DIR = Path(__file__).resolve().parents[1]
REPO_URL = 'https://github.com/elastic/elasticsearch.git'  # Replace with your repository URL
REPO_DIR = ROOT_DIR / 'repositories' / 'elasticsearch'  # Main local directory for the repository
DATASET_FOLDER = ROOT_DIR / 'dataset' / 'java' / 'elasticsearch'
OUTPUT_CSV = ROOT_DIR / 'expanded_dataset.csv'  # Path for the output CSV file
CK_JAR_PATH = ROOT_DIR / "third_party" / "ck-ck-0.7.0" / "target" / 'ck-0.7.0-jar-with-dependencies.jar'  # Path to the CK jar file


NUM_WORKERS = 4  # Number of parallel worker threads
print_lock = threading.Lock()

logging.basicConfig(filename='logfile.log', level=logging.INFO, format='[%(asctime)s] [%(threadName)s] %(message)s')

def safe_print(*args, **kwargs):
    with print_lock:
        message = ' '.join(map(str, args))
        print(f"[{datetime.datetime.now()}] [{threading.current_thread().name}] {message}", **kwargs)
        logging.info(message)

def find_csv_file(folder: Path) -> Path:
    for file_name in os.listdir(folder):
        if file_name.endswith('.csv'):
            return folder / file_name
    raise FileNotFoundError("No CSV file found in the specified folder.")

def clone_repository(repo_url: str, repo_dir: Path):
    """
    Clones the repository if it doesn't exist locally.
    """
    if not repo_dir.exists():
        safe_print(f"Cloning repository into {repo_dir}...")
        Repo.clone_from(repo_url, str(repo_dir))
    else:
        safe_print(f"Repository already cloned at {repo_dir}.")

def copy_repository(original_repo_dir: Path, worker_id: int) -> Path:
    """
    Creates a copy of the repository directory for a specific worker.
    """
    worker_repo_dir = ROOT_DIR / 'repositories' / f'repo_worker_{worker_id}'
    if worker_repo_dir.exists():
        # Update the repository
        safe_print(f"Updating repository for worker {worker_id}...")
        repo = Repo(worker_repo_dir)
        try:
            repo.git.fetch()
            repo.git.reset('--hard', 'origin/main')
            repo.git.clean('-fdx')
        except GitCommandError as e:
            safe_print(f"Error updating repository for worker {worker_id}: {e}")
            return None
    else:
        # Copy the repository from the original repository directory
        safe_print(f"Copying repository for worker {worker_id} from local copy at {original_repo_dir}...")
        shutil.copytree(original_repo_dir, worker_repo_dir)

    return worker_repo_dir

def checkout_commit(repo: Repo, commit_hash: str):
    """
    Checks out the repository at the specified commit.
    """
    try:
        repo.git.checkout(commit_hash, force=True)
    except GitCommandError as e:
        safe_print(f"Error checking out commit {commit_hash}: {e}")

def run_ck(repo_dir: Path, output_dir: Path) -> pd.DataFrame:
    """
    Runs the CK tool on the specified repository directory.
    Returns a DataFrame containing the metrics.
    """
    use_jars = "false"
    max_files_per_partition = 0
    variables_and_fields = "false"

    # Ensure output directory exists
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        completed_process = subprocess.run(
            ['java', '-jar', str(CK_JAR_PATH), str(repo_dir), use_jars, str(max_files_per_partition), variables_and_fields, str(output_dir)],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
    except subprocess.CalledProcessError as e:
        safe_print(f"Error running CK: {e.stderr.decode()}")
        return pd.DataFrame()

    # Correct output file names if needed
    # Because CK does not put results into correct folder provided for some reason
    output_base = str(output_dir)
    if os.path.exists(output_base + 'class.csv'):
        os.rename(output_base + 'class.csv', output_dir / 'class.csv')
    if os.path.exists(output_base + 'method.csv'):
        os.rename(output_base + 'method.csv', output_dir / 'method.csv')
    if os.path.exists(output_base + 'variable.csv'):
        os.rename(output_base + 'variable.csv', output_dir / 'variable.csv')

    class_csv_path = output_dir / 'class.csv'
    if class_csv_path.exists():
        metrics_df = pd.read_csv(class_csv_path)
        return metrics_df
    else:
        safe_print(f"CK output file not found at {class_csv_path}")
        return pd.DataFrame()

analyzed_commits = set()
processing_commits = set()
analyzed_commits_lock = threading.Lock()
processing_commits_lock = threading.Lock()

def load_analyzed_commits():
    """
    Load analyzed commits from existing CSV filenames in DATASET_FOLDER/CK.
    """
    ck_folder = DATASET_FOLDER / 'CK'
    if not ck_folder.exists():
        return
    for filename in os.listdir(ck_folder):
        if filename.endswith('.csv'):
            commit_hash = filename.replace('.csv', '')
            with analyzed_commits_lock:
                analyzed_commits.add(commit_hash)

def mark_commit_analyzed(commit_hash: str):
    """
    Add a commit hash to the set of analyzed commits.
    """
    with analyzed_commits_lock:
        analyzed_commits.add(commit_hash)

def analyze_commit(commit_hash: str, repo: Repo, worker_repo_dir: Path):
    """
    Analyze the specified commit if it has not been analyzed yet.
    """
    with analyzed_commits_lock:
        if commit_hash in analyzed_commits:
            safe_print(f"Commit {commit_hash} already analyzed.")
            return
    with processing_commits_lock:
        if commit_hash in processing_commits:
            safe_print(f"Commit {commit_hash} is being processed by another thread.")
            return
        processing_commits.add(commit_hash)

    try:
        safe_print(f"Processing commit {commit_hash}...")

        # Checkout the current commit
        checkout_commit(repo, commit_hash)

        # Run CK tool for the current commit
        output_dir = ROOT_DIR / f'ck_metrics_output_{commit_hash}'
        metrics_df = run_ck(worker_repo_dir, output_dir)

        # Add commit hash to metrics DataFrame
        metrics_df['commit'] = commit_hash

        # Save metrics DataFrame to CSV (current commit)
        commit_output_csv = DATASET_FOLDER / 'CK' / f'{commit_hash}.csv'
        commit_output_csv.parent.mkdir(parents=True, exist_ok=True)
        metrics_df.to_csv(commit_output_csv, index=False)

        # Mark commit as analyzed
        mark_commit_analyzed(commit_hash)
    except Exception as e:
        safe_print(f"Error while processing commit {commit_hash}: {e}")
    finally:
        with processing_commits_lock:
            processing_commits.remove(commit_hash)

        # Clean up CK output
        if output_dir.exists():
            shutil.rmtree(output_dir)

def process_commits(df: pd.DataFrame, worker_id: int):
    """
    Process the commits assigned to this worker and write results to a partial CSV.
    """
    worker_repo_dir = copy_repository(REPO_DIR, worker_id)
    repo = Repo(worker_repo_dir)
    #checkout_commit(repo, 'main')

    commit_number = 0
    for index, row in df.iterrows():
        commit_number += 1
        commit_hash = row['commit_hash']
        safe_print(f"Processing commit {commit_number} of {len(df)}: {commit_hash}")
        
        analyze_commit(commit_hash, repo, worker_repo_dir) 

        # Get the parent commit and run CK again
        try:
            commit_obj = repo.commit(commit_hash)
            if commit_obj.parents:
                parent_commit_hash = commit_obj.parents[0].hexsha
                analyze_commit(parent_commit_hash, repo, worker_repo_dir)
            else:
                safe_print(f"Commit {commit_hash} has no parent, skipping parent CK run.")
        except Exception as e:
            safe_print(f"Error while processing commit {parent_commit_hash}, parent commit of {commit_hash}: {e}")


    #shutil.rmtree(worker_repo_dir)
    #safe_print(f"[Worker {worker_id}] Removed repository copy at {worker_repo_dir}.")

def save_commit_history():
    """
    Saves the commit history into a single file in the DATASET_FOLDER folder.
    Each line contains a commit hash, starting from the latest commit to the earliest.
    """
    repo = Repo(REPO_DIR)
    commit_history_file = DATASET_FOLDER / 'commit_history.txt'

    with open(commit_history_file, 'w') as f:
        for commit in repo.iter_commits('main'):
            f.write(f"{commit.hexsha}\n")

def main():
    # Clone the repository if necessary
    start_time = datetime.datetime.now()
    safe_print("Start time: ", start_time)

    safe_print("Cloning repository...")
    clone_repository(REPO_URL, REPO_DIR)

    # Load analyzed commits before processing
    load_analyzed_commits()

    safe_print("Saving commit history...")
    save_commit_history()

    # Find the dataset CSV file
    try:
        dataset_csv_path = find_csv_file(DATASET_FOLDER)
    except FileNotFoundError as e:
        safe_print(str(e))
        return

    # Read the dataset
    df = pd.read_csv(dataset_csv_path)
    
    # Use fraction of the dataset for testing (adjust as needed)
    print(len(df))
    #df = df.sample(frac=0.0001, random_state=42)
    #print(len(df))

    # remove commits that are already analyzed from df
    df = df[~df['commit_hash'].isin(analyzed_commits)]

    # Split the dataframe into parts for each worker
    df_splits = np.array_split(df, NUM_WORKERS)

    threads = []

    for worker_id, df_part in enumerate(df_splits):
        t = threading.Thread(target=process_commits, args=(df_part.copy(), worker_id), name=f"Worker-{worker_id}")
        t.start()
        threads.append(t)

    # Wait for all threads to finish
    for t in threads:
        t.join()

    safe_print("All workers finished processing commits.")

    end_time = datetime.datetime.now()
    safe_print("End time: ", end_time)
    safe_print(f"Processed {len(df)} commits.")
    safe_print(f"Averaged {len(df) / NUM_WORKERS} commits per worker.")
    safe_print(f"Total time: {end_time - start_time}")
    safe_print(f"Average time per commit: {(end_time - start_time) / len(df)}")

if __name__ == '__main__':
    main()
