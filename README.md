# git2parquet

[![npm](https://img.shields.io/npm/v/git2parquet)](https://www.npmjs.com/package/git2parquet)
[![mit license](https://img.shields.io/badge/License-MIT-orange.svg)](https://opensource.org/licenses/MIT)
[![dependencies](https://img.shields.io/badge/Dependencies-2-blueviolet)](https://www.npmjs.com/package/git2parquet?activeTab=dependencies)

A command-line tool to convert git commit history to Parquet format, including unified diffs for data analysis and AI applications.

## Installation

```bash
npm install -g git2parquet
```

## Usage

### Command Line

```bash
# Export git history of current repo to gitlog.parquet
git2parquet

# Export to custom filename
git2parquet commits.parquet

# Export and open with hyperparam
git2parquet --open

# Export to custom file and open with hyperparam
git2parquet commits.parquet --open
```

## Output Schema

The generated Parquet file contains the following columns:

- `hash` (STRING): Git commit hash
- `authorName` (STRING): Author's name
- `authorEmail` (STRING): Author's email address
- `date` (TIMESTAMP): Commit date in ISO format
- `subject` (STRING): Commit message subject line
- `diff` (STRING): Unified diff showing file changes

## Requirements

- Node.js
- Must be run from within a git repository
- Git must be available in PATH

## Options

- `--help`, `-h`: Show help message
- `--open`: Open the generated Parquet file with hyperparam after export

## Use Cases

- Analyzing code change patterns over time
- Training ML models on code evolution
- Creating datasets for software engineering research
- Building commit history dashboards

## Hyperparam

[Hyperparam](https://hyperparam.app) is a tool for exploring and curating AI datasets. The Hyperparam CLI (`npx hyperparam`) is a local viewer for ML datasets that launches a small HTTP server and opens your browser to interactively explore the generated git2parquet output file.
