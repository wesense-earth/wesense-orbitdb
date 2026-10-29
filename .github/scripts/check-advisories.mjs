// Compare security advisories published by the upstream projects we depend on against the
// versions in package-lock.json.
//
// Why: Dependabot only raises alerts for advisories in GitHub's reviewed advisory database.
// js-libp2p publishes advisories in its own repository, and several of those never became
// Dependabot alerts here, so `npm audit` said 0 while installed versions were affected.
//
// Writes findings (markdown) to the file named by OUTPUT_FILE and prints them.
// Exit status is 0 either way; the workflow decides what to do with the findings.
//
// Env: GITHUB_TOKEN (read access to public repos), SEMVER_FROM (a path semver can be
// resolved from), LOCKFILE (default package-lock.json), OUTPUT_FILE, ADVISORY_REPOS
// (comma-separated owner/repo list, optional).

import { createRequire } from "node:module";
import { readFileSync, writeFileSync } from "node:fs";

const require = createRequire(process.env.SEMVER_FROM || import.meta.url);
const semver = require("semver");

const REPOS = (process.env.ADVISORY_REPOS || "libp2p/js-libp2p,orbitdb/orbitdb,ipfs/helia")
  .split(",").map((s) => s.trim()).filter(Boolean);
const LOCKFILE = process.env.LOCKFILE || "package-lock.json";

async function advisoriesFor(repo) {
  const out = [];
  for (let page = 1; ; page++) {
    const res = await fetch(
      `https://api.github.com/repos/${repo}/security-advisories?state=published&per_page=100&page=${page}`,
      {
        headers: {
          accept: "application/vnd.github+json",
          "x-github-api-version": "2022-11-28",
          ...(process.env.GITHUB_TOKEN ? { authorization: `Bearer ${process.env.GITHUB_TOKEN}` } : {}),
        },
      },
    );
    if (!res.ok) throw new Error(`${repo}: advisory API returned ${res.status}`);
    const batch = await res.json();
    out.push(...batch);
    if (batch.length < 100) return out;
  }
}

// name -> Set of installed versions, from every node_modules/... entry in the lockfile
function installedVersions() {
  const lock = JSON.parse(readFileSync(LOCKFILE, "utf8"));
  const byName = new Map();
  for (const [path, info] of Object.entries(lock.packages || {})) {
    if (!path.includes("node_modules/") || !info.version) continue;
    const name = info.name || path.slice(path.lastIndexOf("node_modules/") + "node_modules/".length);
    if (!byName.has(name)) byName.set(name, new Set());
    byName.get(name).add(info.version);
  }
  return byName;
}

// GitHub ranges look like ">= 1.0.0, < 2.0.0" or "< 17.0.0"; semver wants spaces, not commas.
function inRange(version, range) {
  return semver.satisfies(version, range.replace(/,/g, " ").replace(/(<=?|>=?|=)\s+/g, "$1"), {
    includePrerelease: true,
  });
}

const installed = installedVersions();
const findings = [];
const unparsable = [];

for (const repo of REPOS) {
  for (const adv of await advisoriesFor(repo)) {
    for (const vuln of adv.vulnerabilities || []) {
      if (vuln.package?.ecosystem !== "npm") continue;
      const versions = installed.get(vuln.package.name);
      if (!versions) continue;
      for (const version of versions) {
        let affected;
        try {
          if (!vuln.vulnerable_version_range) throw new Error("no range");
          affected = inRange(version, vuln.vulnerable_version_range);
        } catch {
          unparsable.push(`${adv.ghsa_id} (${vuln.package.name}@${version}, range "${vuln.vulnerable_version_range}")`);
          continue;
        }
        if (affected) {
          findings.push({
            id: adv.ghsa_id,
            url: adv.html_url,
            severity: adv.severity,
            summary: adv.summary,
            pkg: vuln.package.name,
            version,
            range: vuln.vulnerable_version_range,
            fixed: vuln.patched_versions || "no fixed version listed",
          });
        }
      }
    }
  }
}

const lines = [];
if (findings.length) {
  lines.push("Installed packages affected by advisories published by the upstream projects:", "");
  lines.push("| Advisory | Severity | Package | Installed | Vulnerable | Fixed in |", "|---|---|---|---|---|---|");
  for (const f of findings) {
    lines.push(`| [${f.id}](${f.url}) ${f.summary.replace(/\|/g, "/")} | ${f.severity} | ${f.pkg} | ${f.version} | ${f.range} | ${f.fixed} |`);
  }
}
if (unparsable.length) {
  lines.push("", "Advisories whose version range could not be evaluated (check by hand):", ...unparsable.map((u) => `- ${u}`));
}
const text = lines.join("\n");
console.log(text || `No installed package is affected by advisories published by: ${REPOS.join(", ")}`);
if (process.env.OUTPUT_FILE) writeFileSync(process.env.OUTPUT_FILE, text);
