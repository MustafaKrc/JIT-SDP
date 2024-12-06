import os
import subprocess
import shutil
from pathlib import Path
from typing import List
import pandas as pd

# Import GitPython for repository interaction
from git import Repo, GitCommandError

# Constants
ROOT_DIR = Path(__file__).resolve().parents[1]
REPO_URL = 'https://github.com/elastic/elasticsearch.git'  # Replace with your repository URL
REPO_DIR = ROOT_DIR / 'repositories' / 'elasticsearch'  # Local directory for the repository
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
        print(f"Cloning repository into {repo_dir}...")
        Repo.clone_from(repo_url, str(repo_dir))
    else:
        print(f"Repository already cloned at {repo_dir}.")

def checkout_commit(repo: Repo, commit_hash: str):
    """
    Checks out the repository at the specified commit.
    """
    try:
        repo.git.checkout(commit_hash)
        #print(f"Checked out commit {commit_hash}.")
    except GitCommandError as e:
        print(f"Error checking out commit {commit_hash}: {e}")

def run_ck(repo_dir: Path, output_dir: Path) -> pd.DataFrame:
    """
    Runs the CK tool on the specified repository directory.
    Returns a DataFrame containing the metrics.
    """
    #class_csv_path = output_dir / 'class.csv'
    #metrics_df = pd.read_csv(class_csv_path)
    #return metrics_df

    use_jars = "false" # Use JAR files to analyze Java projects
    max_files_per_partition = 0 # 0 means automatic selection
    variables_and_fields = "false" # Include variables and fields in the analysis

    # Ensure output directory exists
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Run CK
    #print("Running CK tool...")
    try:
        subprocess.run(
            ['java', '-jar', str(CK_JAR_PATH), str(repo_dir), use_jars, str(max_files_per_partition), variables_and_fields, str(output_dir)],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        #print("CK analysis completed.")
    except subprocess.CalledProcessError as e:
        print(f"Error running CK: {e.stderr.decode()}")
        return pd.DataFrame()
    
    # CK outputs have incorrect file paths/names
    # Files are generated as 'ck_metrics_outputclass.csv' instead of 'ck_metrics_output/class.csv'
    # Let's fix by checking exact file names and moving them to correct location

    output_base = str(output_dir)
    # Check for specific file names
    if os.path.exists(output_base + 'class.csv'):
        os.rename(output_base + 'class.csv', output_dir / 'class.csv')
    if os.path.exists(output_base + 'method.csv'):
        os.rename(output_base + 'method.csv', output_dir / 'method.csv')
    if os.path.exists(output_base + 'variable.csv'):
        os.rename(output_base + 'variable.csv', output_dir / 'variable.csv')
    
    # CK outputs several CSV files: class.csv, method.csv, variable.csv, etc.
    # We'll focus on class-level metrics (class.csv)
    class_csv_path = output_dir / 'class.csv'
    if class_csv_path.exists():
        # Read the class metrics CSV file into a DataFrame
        metrics_df = pd.read_csv(class_csv_path)
        return metrics_df
    else:
        print(f"CK output file not found at {class_csv_path}")
        return pd.DataFrame()
    
def get_CK_metrics(files: list[str], CK_metrics_df: pd.DataFrame) -> pd.DataFrame:
    """
    Get the CK metrics for the modified files in the commit
    If there are multiple class metrics, returns the average
    """
    # Get the class metrics for the modified files
    class_metrics = CK_metrics_df[CK_metrics_df['file'].isin(files)]
    
    # Separate numeric and non-numeric columns
    numeric_metrics = class_metrics[CK_METRIC_COLUMNS].select_dtypes(include=['int64', 'float64'])
    non_numeric_metrics = class_metrics[CK_METRIC_COLUMNS].select_dtypes(exclude=['int64', 'float64'])
    
    # Aggregate numeric columns with mean and non-numeric columns with first value
    aggregated_metrics = pd.concat([
        numeric_metrics.mean(),
        non_numeric_metrics.iloc[0] if not non_numeric_metrics.empty else pd.Series()
    ])
    
    return aggregated_metrics

def main():
    # Clone the repository if necessary
    clone_repository(REPO_URL, REPO_DIR)
    repo = Repo(REPO_DIR)
    checkout_commit(repo, 'main')
    
    # Find the dataset CSV file in the specified folder
    dataset_csv_path = find_csv_file(DATASET_FOLDER)
    
    # Read the dataset
    df = pd.read_csv(dataset_csv_path)
    
    # For test purposes, limit the number of commits processed
    #df = df.head(1)
    

    for metric in CK_METRIC_COLUMNS:
        if metric not in df.columns:
            df[metric] = None

    commit_number = 0
    # Process each commit
    for index, row in df.iterrows():
        commit_number += 1
        print(f"\rProcessing commit {commit_number} of {len(df)}...", end='\r')

        commit_hash = row['commit_hash']
        #print(f"\nProcessing commit {commit_hash}...")
    
        # Checkout the commit
        checkout_commit(repo, commit_hash)
    
        # Run CK tool
        output_dir = ROOT_DIR / 'ck_metrics_output'
        metrics_df = run_ck(REPO_DIR, output_dir)

        # Remove the path up until elasticsearch folder in metrics_df['file']
        metrics_df['file'] = metrics_df['file'].apply(lambda x: os.path.relpath(x, start=str(REPO_DIR)))
        # Replace '\' with '/' in metrics_df['file']
        metrics_df['file'] = metrics_df['file'].apply(lambda x: x.replace('\\', '/'))
    
        if metrics_df.empty:
            print(f"No metrics collected for commit {commit_hash}.")
            continue

        # Get the changed files for the commit and remove preceeding and trailing commas
        changed_files_str = row['fileschanged']
        changed_files = [f.strip().strip(',') for f in changed_files_str.split('CAS_DELIMITER') if f.strip()]
        #print(f"Changed files: {changed_files}")

        # Get class metrics of modified files
        modified_files_metrics = get_CK_metrics(changed_files, metrics_df)

        # Update the DataFrame with the metrics
        for metric in CK_METRIC_COLUMNS:
            df.at[index, metric] = modified_files_metrics.get(metric, None)
    
        # Clean up CK output directory
        shutil.rmtree(output_dir)
        
    checkout_commit(repo, 'main')

    # Save the expanded dataset
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"\nExpanded dataset saved to {OUTPUT_CSV}.")

if __name__ == '__main__':
    main()
