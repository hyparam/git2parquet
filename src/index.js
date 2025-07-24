import { execSync } from 'node:child_process'
import { resolve, basename } from 'node:path'
import { parquetWriteFile } from 'hyparquet-writer'

const defaultFilename = 'gitlog.parquet'

/**
 * Return basic repository information for inclusion in Parquet metadata.
 * @returns {{name:string, branch:string, head:string, remote:string}}
 */
function readRepoInfo() {
  // throws if not inside a repository – caller already checks this
  const root = execSync('git rev-parse --show-toplevel', { encoding: 'utf8', stdio: ['ignore', 'pipe', 'inherit'] }).trim()
  const name = basename(root)

  let branch = ''
  try {
    branch = execSync('git rev-parse --abbrev-ref HEAD', { encoding: 'utf8', stdio: ['ignore', 'pipe', 'inherit'] }).trim()
  } catch {}

  let head = ''
  try {
    head = execSync('git rev-parse HEAD', { encoding: 'utf8', stdio: ['ignore', 'pipe', 'inherit'] }).trim()
  } catch {}

  let remote = ''
  try {
    remote = execSync('git config --get remote.origin.url', { encoding: 'utf8', stdio: ['ignore', 'pipe', 'inherit'] }).trim()
  } catch {}

  return { name, branch, head, remote }
}

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
      {
        encoding: 'utf8',
        stdio: ['ignore', 'pipe', 'inherit'],
        maxBuffer: 50 * 1024 * 1024 // 50MB buffer to handle large repositories
      }
    )
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    throw new Error(`Failed to read git log: ${message}`)
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
        {
          encoding: 'utf8',
          stdio: ['ignore', 'pipe', 'inherit'],
          maxBuffer: 50 * 1024 * 1024 // 50MB buffer to handle large diffs
        }
      ).trim()
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      console.warn(`Warning: Could not get diff for commit ${hash}: ${message}`)
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
 * Adds repository metadata via `kvMetadata`.
 * @param {{filename?:string}} [opts]
 * @returns {Promise<{commitCount:number, filename:string}>}
 */
export async function writeGitLogParquet(opts = {}) {
  if (opts && typeof opts !== 'object') {
    throw new Error('Options must be an object')
  }

  if (opts.filename && typeof opts.filename !== 'string') {
    throw new Error('Filename must be a string')
  }

  // collect data
  const rows = readGitLogWithDiffs()
  if (!rows.length) {
    throw new Error('No commits found in repository')
  }

  // repo metadata
  const repo = readRepoInfo()

  // format metadata for hyparquet-writer
  const kvMetadata = Object.entries({
    repo_name: repo.name,
    branch: repo.branch,
    head: repo.head,
    remote: repo.remote,
  }).map(([key, value]) => ({ key, value }))

  const filename = resolve(opts.filename ?? defaultFilename)

  try {
    await parquetWriteFile({
      filename,
      columnData: toColumnData(rows),
      kvMetadata,
    })
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    throw new Error(`Failed to write parquet file: ${message}`)
  }

  return { commitCount: rows.length, filename }
}
