import os
import subprocess
import shutil
from pathlib import Path
from typing import List
import pandas as pd
import threading
from git import Repo, GitCommandError

# Constants
ROOT_DIR = Path(__file__).resolve().parents[1]
REPO_URL = 'https://github.com/elastic/elasticsearch.git'  # Replace with your repository URL
REPO_DIR = ROOT_DIR / 'repositories' / 'elasticsearch'  # Main local directory for the repository
DATASET_FOLDER = ROOT_DIR / 'dataset' / 'java' / 'elasticsearch'
OUTPUT_CSV = ROOT_DIR / 'expanded_dataset.csv'  # Path for the output CSV file
CK_JAR_PATH = ROOT_DIR / "third_party" / "ck-ck-0.7.0" / "target" / 'ck-0.7.0-jar-with-dependencies.jar'  # Path to the CK jar file

CK_METRIC_COLUMNS = [
    'file', 'class', 'type', 'cbo', 'cboModified', 'fanin', 'fanout', 'wmc', 'dit', 'noc', 'rfc',
    'lcom', 'lcom*', 'tcc', 'lcc', 'totalMethodsQty', 'staticMethodsQty', 'publicMethodsQty',
    'privateMethodsQty', 'protectedMethodsQty', 'defaultMethodsQty', 'visibleMethodsQty',
    'abstractMethodsQty', 'finalMethodsQty', 'synchronizedMethodsQty', 'totalFieldsQty',
    'staticFieldsQty', 'publicFieldsQty', 'privateFieldsQty', 'protectedFieldsQty',
    'defaultFieldsQty', 'finalFieldsQty', 'synchronizedFieldsQty', 'nosi', 'loc', 'returnQty',
    'loopQty', 'comparisonsQty', 'tryCatchQty', 'parenthesizedExpsQty', 'stringLiteralsQty',
    'numbersQty', 'assignmentsQty', 'mathOperationsQty', 'variablesQty', 'maxNestedBlocksQty',
    'anonymousClassesQty', 'innerClassesQty', 'lambdasQty', 'uniqueWordsQty', 'modifiers',
    'logStatementsQty'
]

NUM_WORKERS = 8
print_lock = threading.Lock()


def safe_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)


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
    worker_repo_dir = ROOT_DIR / 'repositories' / f'elasticsearch_{worker_id}'
    if worker_repo_dir.exists():
        pass
        #shutil.rmtree(worker_repo_dir)
    # delete this later..
    else:
        shutil.copytree(original_repo_dir, worker_repo_dir)
    return worker_repo_dir


def checkout_commit(repo: Repo, commit_hash: str):
    """
    Checks out the repository at the specified commit.
    """
    try:
        repo.git.checkout(commit_hash)
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
        subprocess.run(
            ['java', '-jar', str(CK_JAR_PATH), str(repo_dir), use_jars, str(max_files_per_partition), variables_and_fields, str(output_dir)],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
    except subprocess.CalledProcessError as e:
        safe_print(f"Error running CK: {e.stderr.decode()}")
        return pd.DataFrame()

    # Correct output file names if needed
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


def get_CK_metrics(files: list[str], CK_metrics_df: pd.DataFrame) -> pd.DataFrame:
    """
    Get the CK metrics for the modified files in the commit.
    If multiple classes match, we aggregate metrics by mean for numeric columns.
    """
    class_metrics = CK_metrics_df[CK_metrics_df['file'].isin(files)]

    numeric_metrics = class_metrics[CK_METRIC_COLUMNS].select_dtypes(include=['int64', 'float64'])
    non_numeric_metrics = class_metrics[CK_METRIC_COLUMNS].select_dtypes(exclude=['int64', 'float64'])

    # Aggregate numeric columns by mean
    aggregated_numeric = numeric_metrics.mean() if not numeric_metrics.empty else pd.Series([None] * len(numeric_metrics.columns), index=numeric_metrics.columns)

    aggregated_metrics = pd.concat([aggregated_numeric, non_numeric_metrics])
    return aggregated_metrics


def process_commits(df: pd.DataFrame, worker_id: int, partial_output_csv: Path):
    """
    Process the commits assigned to this worker and write results to a partial CSV.
    """
    worker_repo_dir = copy_repository(REPO_DIR, worker_id)
    repo = Repo(worker_repo_dir)
    checkout_commit(repo, 'main')

    # Add missing CK metric columns if not present
    for metric in CK_METRIC_COLUMNS:
        if metric not in df.columns:
            df[metric] = None

    # Process each commit in the given dataframe partition
    commit_number = 0
    for index, row in df.iterrows():
        commit_number += 1
        safe_print(f"[Worker {worker_id}] Processing commit {commit_number} of {len(df)}...")
        commit_hash = row['commit_hash']

        checkout_commit(repo, commit_hash)

        output_dir = ROOT_DIR / 'ck_metrics_output' / f'worker_{worker_id}'
        metrics_df = run_ck(worker_repo_dir, output_dir)

        # Normalize file paths
        if not metrics_df.empty:
            metrics_df['file'] = metrics_df['file'].apply(lambda x: os.path.relpath(x, start=str(worker_repo_dir)))
            metrics_df['file'] = metrics_df['file'].apply(lambda x: x.replace('\\', '/'))

        if metrics_df.empty:
            safe_print(f"[Worker {worker_id}] No metrics collected for commit {commit_hash}.")
            continue

        changed_files_str = row['fileschanged']
        changed_files = [f.strip().strip(',') for f in changed_files_str.split('CAS_DELIMITER') if f.strip()]
        modified_files_metrics = get_CK_metrics(changed_files, metrics_df)

        # Update this worker's df partition
        for metric in CK_METRIC_COLUMNS:
            df.at[index, metric] = modified_files_metrics.get(metric, None)

        # Clean up CK output
        shutil.rmtree(output_dir)

    checkout_commit(repo, 'main')

    # Save this worker's partial results
    df.to_csv(partial_output_csv, index=False)
    safe_print(f"[Worker {worker_id}] Partial results saved to {partial_output_csv}.")


def main():
    # Clone the repository if necessary
    clone_repository(REPO_URL, REPO_DIR)

    # Find the dataset CSV file
    dataset_csv_path = find_csv_file(DATASET_FOLDER)

    # Read the dataset
    df = pd.read_csv(dataset_csv_path)

    # use %0.1 of dataset for testing
    df = df.sample(frac=0.001, random_state=42)

    print(len(df))

    # Split the dataframe into parts for each worker
    df_splits = np.array_split(df, NUM_WORKERS)

    threads = []
    partial_results = []
    for worker_id, df_part in enumerate(df_splits):
        partial_output_csv = ROOT_DIR / f'expanded_dataset_part_{worker_id}.csv'
        t = threading.Thread(target=process_commits, args=(df_part.copy(), worker_id, partial_output_csv))
        t.start()
        threads.append(t)
        partial_results.append(partial_output_csv)

    # Wait for all threads to finish
    for t in threads:
        t.join()

    # Merge partial results
    final_df = pd.DataFrame()
    for part_csv in partial_results:
        part_df = pd.read_csv(part_csv)
        final_df = pd.concat([final_df, part_df], ignore_index=True)

    # Save the expanded dataset
    final_df.to_csv(OUTPUT_CSV, index=False)
    safe_print(f"Expanded dataset saved to {OUTPUT_CSV}.")

    # Cleanup copied repositories
    for worker_id in range(NUM_WORKERS):
        worker_repo_dir = ROOT_DIR / 'repositories' / f'elasticsearch_{worker_id}'
        if worker_repo_dir.exists():
            shutil.rmtree(worker_repo_dir)
            safe_print(f"Removed {worker_repo_dir}")

if __name__ == '__main__':
    import numpy as np
    main()
