import os
import csv
import subprocess
import shutil
import tempfile
from pathlib import Path
from typing import List, Dict
import pandas as pd

# Import GitPython for repository interaction
from git import Repo, GitCommandError

# Constants
ROOT_DIR = Path(__file__).resolve().parents[1]
REPO_URL = 'https://github.com/elastic/elasticsearch.git'  # Replace with your repository URL
REPO_DIR = 'repositories/elasticsearch'  # Local directory for the repository
DATASET_FOLDER = 'dataset/java/elasticsearch'
OUTPUT_CSV = 'expanded_dataset.csv'  # Path for the output CSV file

CODEQL_BIN = 'codeql'  # Path to CodeQL CLI
CODEQL_DB = 'codeql_database'  # Temporary CodeQL database directory
QUERY_FILE = 'queries/java/metrics.ql'  # Path to your custom CodeQL query file

def find_csv_file(folder):
    for file_name in os.listdir(folder):
        if file_name.endswith('.csv'):
            return os.path.join(folder, file_name)
    raise FileNotFoundError("No CSV file found in the specified folder.")

def clone_repository(repo_url: str, repo_dir: str):
    """
    Clones the repository if it doesn't exist locally.
    """
    if not os.path.exists(repo_dir):
        print(f"Cloning repository into {repo_dir}...")
        Repo.clone_from(repo_url, repo_dir)
    else:
        print(f"Repository already cloned at {repo_dir}.")

def checkout_commit(repo: Repo, commit_hash: str):
    """
    Checks out the repository at the specified commit.
    """
    try:
        repo.git.checkout(commit_hash)
        print(f"Checked out commit {commit_hash}.")
    except GitCommandError as e:
        print(f"Error checking out commit {commit_hash}: {e}")

def create_codeql_database(repo_dir: str):
    """
    Creates a CodeQL database for the repository.
    """
    if os.path.exists(CODEQL_DB):
        shutil.rmtree(CODEQL_DB)  # Clean up existing database
    # Create the CodeQL databasefolder
    os.makedirs(CODEQL_DB, exist_ok=True)
    print("Creating CodeQL database...")
    print( "Command: ", [CODEQL_BIN, 'database', 'create', CODEQL_DB, '--language=java', '--source-root', "./"+repo_dir])
    print("CWD: ", ROOT_DIR)
    subprocess.run(
        [CODEQL_BIN, 'database', 'create', CODEQL_DB, '--language=java', '--source-root', "./"+repo_dir],
        cwd=ROOT_DIR,
        check=True
    )
    print("CodeQL database created.")

def run_codeql_query(query_file: str) -> pd.DataFrame:
    """
    Runs the CodeQL query and parses the output into a DataFrame.
    """
    output_file = None
    decoded_csv = None
    try:
        output_file = tempfile.NamedTemporaryFile(suffix='.bqrs')
        decoded_csv = tempfile.NamedTemporaryFile(suffix='.csv')
        
        # Run the CodeQL query
        print("Running CodeQL query...")
        subprocess.run(
            [CODEQL_BIN, 'query', 'run', query_file, '--database', CODEQL_DB, '--output', output_file.name],
            cwd=ROOT_DIR,
            check=True,
        )
        
        # Decode the query results to CSV
        subprocess.run(
            [CODEQL_BIN, 'bqrs', 'decode', '--format=csv', '--output', decoded_csv.name, output_file.name],
            cwd=ROOT_DIR,
            check=True
        )
        
        # Load the CSV into a DataFrame
        return pd.read_csv(decoded_csv.name)
    finally:
        # Clean up temporary files
        if output_file and os.path.exists(output_file.name):
            os.unlink(output_file.name)
        if decoded_csv and os.path.exists(decoded_csv.name):
            os.unlink(decoded_csv.name)

def main():
    # Clone the repository if necessary
    clone_repository(REPO_URL, REPO_DIR)
    repo = Repo(REPO_DIR)
    #repo.git.checkout('main')  # Checkout to the main branch

    # Find the dataset CSV file in the specified folder
    dataset_csv_path = find_csv_file(DATASET_FOLDER)

    # Read the dataset
    df = pd.read_csv(dataset_csv_path)

    # for test purposes, only have 10 commits in the dataset
    df = df.head(1)

    # Prepare columns for new metrics
    df['WMC'] = None  # Weighted Methods per Class
    df['NOM'] = None  # Number of Methods
    df['NOF'] = None  # Number of Fields

    # Process each commit
    for index, row in df.iterrows():
        commit_hash = row['commit_hash']
        print(f"Processing commit {commit_hash}...")

        # Checkout the commit
        checkout_commit(repo, commit_hash)

        print("commit_hash: ", commit_hash)

        # Create CodeQL database
        #create_codeql_database(REPO_DIR)

        # Run CodeQL query
        try:
            metrics_df = run_codeql_query(QUERY_FILE)
        except Exception as e:
            print(f"Error running CodeQL query: {e}")
            continue

        # Aggregate metrics from query results
        #total_wmc = metrics_df['WMC'].sum()
        total_nom = metrics_df['NOM'].sum()
        total_nof = metrics_df['NOF'].sum()
        count = len(metrics_df)

        if count > 0:
            #df.at[index, 'WMC'] = total_wmc / count
            df.at[index, 'NOM'] = total_nom / count
            df.at[index, 'NOF'] = total_nof / count

    # Checkout to the main branch
    #repo.git.checkout('main')

    # Save the expanded dataset
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Expanded dataset saved to {OUTPUT_CSV}.")

if __name__ == '__main__':
    print("no no no... you dont want to run this now")
    #main()
