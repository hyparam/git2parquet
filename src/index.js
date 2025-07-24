#!/usr/bin/env node

import { execSync } from 'node:child_process'
import { resolve } from 'node:path'
import { parquetWriteFile } from 'hyparquet-writer'

const defaultFilename = 'gitlog.parquet'

/**
 * Return commit objects including unified diff.
 * @returns {{hash:string, authorName:string, authorEmail:string, date:string, subject:string, diff:string}[]}
 */
function readGitLogWithDiffs() {
  try {
    // Check if we're in a git repository
    execSync('git rev-parse --git-dir', { stdio: 'ignore' })
  } catch {
    throw new Error('Not in a git repository')
  }

  const sep = '%x09' // literal tab
  const format = ['%H', '%an', '%ae', '%ad', '%s'].join(sep)

  let raw
  try {
    raw = execSync(
      `git log --pretty=format:${format} --date=iso-strict`,
      { encoding: 'utf8', stdio: ['ignore', 'pipe', 'inherit'] }
    )
  } catch (error) {
    throw new Error(`Failed to read git log: ${error.message}`)
  }

  if (!raw.trim()) {
    return []
  }

  return raw.trim().split('\n').map(line => {
    const parts = line.split('\t')
    if (parts.length < 5) {
      throw new Error(`Invalid git log format: ${line}`)
    }

    const [hash, authorName, authorEmail, date, ...subjectParts] = parts
    const subject = subjectParts.join('\t') // Handle subjects with tabs

    let diff
    try {
      // `--pretty=format:` suppresses header lines so we get only the patch
      diff = execSync(
        `git show --patch --unified=0 --no-color --pretty=format: ${hash}`,
        { encoding: 'utf8', stdio: ['ignore', 'pipe', 'inherit'] }
      ).trim()
    } catch (error) {
      console.warn(`Warning: Could not get diff for commit ${hash}: ${error.message}`)
      diff = ''
    }

    return { hash, authorName, authorEmail, date, subject, diff }
  })
}

/**
 * Convert rows into column‑oriented data for `hyparquet-writer`.
 * @param {ReturnType<typeof readGitLogWithDiffs>} rows
 * @returns {import('hyparquet-writer').ColumnSource[]}
 */
function toColumnData(rows) {
  const hash = /** @type {string[]} */ ([])
  const authorName = /** @type {string[]} */ ([])
  const authorEmail = /** @type {string[]} */ ([])
  const date = /** @type {Date[]} */ ([])
  const subject = /** @type {string[]} */ ([])
  const diff = /** @type {string[]} */ ([])

  for (const r of rows) {
    hash.push(r.hash)
    authorName.push(r.authorName)
    authorEmail.push(r.authorEmail)
    date.push(new Date(r.date))
    subject.push(r.subject)
    diff.push(r.diff)
  }

  return [
    { name: 'hash',       data: hash,       type: 'STRING' },
    { name: 'authorName', data: authorName, type: 'STRING' },
    { name: 'authorEmail', data: authorEmail, type: 'STRING' },
    { name: 'date',       data: date,       type: 'TIMESTAMP' },
    { name: 'subject',    data: subject,    type: 'STRING' },
    { name: 'diff',       data: diff,       type: 'STRING' }
  ]
}

/**
 * Write the repository history (including diffs) to a Parquet file.
 * @param {{filename?:string}} [opts]
 * @returns {Promise<{commitCount: number, filename: string}>}
 */
export async function writeGitLogParquet(opts = {}) {
  if (opts && typeof opts !== 'object') {
    throw new Error('Options must be an object')
  }

  if (opts.filename && typeof opts.filename !== 'string') {
    throw new Error('Filename must be a string')
  }

  const rows = readGitLogWithDiffs()
  if (!rows.length) {
    throw new Error('No commits found in repository')
  }

  const filename = resolve(opts.filename ?? defaultFilename)

  try {
    await parquetWriteFile({
      filename,
      columnData: toColumnData(rows)
    })
  } catch (error) {
    throw new Error(`Failed to write parquet file: ${error.message}`)
  }

  return { commitCount: rows.length, filename }
}

/**
 * Parse command line arguments
 */
function parseCliArgs() {
  const args = process.argv.slice(2)
  const options = {}

  for (let i = 0; i < args.length; i++) {
    const arg = args[i]

    if (arg === '--help' || arg === '-h') {
      console.log(`git2parquet

Usage: git2parquet [filename] [options]

Arguments:
  filename              Output parquet filename (default: gitlog.parquet)

Options:
  -h, --help           Show this help message
      --open           Open the parquet file with hyperparam after export

Examples:
  git2parquet                           # Export to gitlog.parquet
  git2parquet commits.parquet           # Export to commits.parquet
  git2parquet --open                    # Export and open with hyperparam
  git2parquet commits.parquet --open    # Export to commits.parquet and open`)
      process.exit(0)
    }

    if (arg === '--open') {
      options.open = true
      continue
    }

    if (arg.startsWith('-')) {
      console.error(`Error: Unknown option '${arg}'`)
      console.error('Use --help for usage information')
      process.exit(1)
    }

    if (options.filename) {
      console.error('Error: Multiple filenames provided')
      console.error('Use --help for usage information')
      process.exit(1)
    }

    options.filename = arg
  }

  return options
}

/* CLI entry point */
if (import.meta.url.startsWith('file:') && process.argv[1] && import.meta.url.includes(process.argv[1].split('/').pop())) {
  const options = parseCliArgs()

  writeGitLogParquet(options).then(result => {
    const cwd = process.cwd()
    const localPath = result.filename.startsWith(cwd)
      ? result.filename.slice(cwd.length + 1)
      : result.filename
    const filename = result.filename.split('/').pop()

    console.log(`✓ Exported ${result.commitCount} commits to ${filename}.`)

    if (options.open) {
      console.log(`Opening ${filename} with hyperparam...`)
      try {
        execSync(`npx hyperparam ${localPath}`, { stdio: 'inherit' })
      } catch (error) {
        console.error(`Failed to open with hyperparam: ${error.message}`)
        console.log(`\nView it manually with:\n  npx hyperparam ${localPath}\n`)
      }
    } else {
      console.log(`View it with:\n  npx hyperparam ${localPath}\n`)
    }
  }).catch(err => {
    console.error(`Error: ${err.message}`)
    process.exit(1)
  })
}
