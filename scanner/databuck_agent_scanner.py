import os
import re
import ast
import csv
import json
import argparse
from datetime import datetime, UTC
from typing import Any, Dict, List, Optional


IGNORE_DIRS = {
    ".git",
    "__pycache__",
    ".venv",
    "venv",
    "env",
    "node_modules",
    "target",
    "build",
    "dist",
    "scan_output",
    "scan_outpu",
    "scanner",
}


SUPPORTED_EXTENSIONS = {
    ".py",
    ".yaml",
    ".yml",
    ".json",
    ".txt",
    ".md",
}


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def read_file(file_path: str) -> str:
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            return file.read()
    except UnicodeDecodeError:
        with open(file_path, "r", encoding="latin-1") as file:
            return file.read()


def discover_code_files(repo_path: str) -> List[str]:
    files_found = []

    for root, dirs, files in os.walk(repo_path):
        dirs[:] = [d for d in dirs if d not in IGNORE_DIRS]

        for file_name in files:
            ext = os.path.splitext(file_name)[1].lower()

            if ext in SUPPORTED_EXTENSIONS:
                files_found.append(os.path.join(root, file_name))

    return files_found


def extract_python_assignments(file_content: str) -> Dict[str, Any]:
    """
    Evidence collection only.
    This is not the final decision layer.

    Extracts simple constants such as:
    self.agent_name = "BigQuery Customer Insight Agent"
    self.project = "databuckk8s"
    self.dataset = "new_data_set"
    self.table_name = "File_RM_Microsect"
    """

    assignments = {}

    try:
        tree = ast.parse(file_content)

        for node in ast.walk(tree):
            if not isinstance(node, ast.Assign):
                continue

            value = None

            if isinstance(node.value, ast.Constant):
                value = node.value.value

            elif isinstance(node.value, ast.JoinedStr):
                value = "dynamic_f_string"

            if value is None:
                continue

            for target in node.targets:
                key = None

                if isinstance(target, ast.Attribute):
                    key = target.attr

                elif isinstance(target, ast.Name):
                    key = target.id

                if key:
                    assignments[key] = value

    except Exception:
        pass

    return assignments


def extract_python_class_names(file_content: str) -> List[str]:
    try:
        tree = ast.parse(file_content)

        return [
            node.name
            for node in ast.walk(tree)
            if isinstance(node, ast.ClassDef)
        ]

    except Exception:
        return []


def extract_python_function_names(file_content: str) -> List[str]:
    try:
        tree = ast.parse(file_content)

        return [
            node.name
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef)
        ]

    except Exception:
        return []


def detect_language(file_path: str) -> str:
    ext = os.path.splitext(file_path)[1].lower()

    if ext == ".py":
        return "Python"

    if ext in {".js", ".ts"}:
        return "JavaScript/TypeScript"

    if ext in {".yaml", ".yml"}:
        return "YAML"

    if ext == ".json":
        return "JSON"

    if ext == ".md":
        return "Markdown"

    return "Unknown"


def collect_sql_blocks(file_content: str) -> List[str]:
    """
    Collects SQL-like blocks as evidence.
    This is not final extraction.
    Master Agent will analyze these blocks.
    """

    queries = []

    triple_quote_pattern = r'("""[\s\S]*?"""|\'\'\'[\s\S]*?\'\'\')'
    blocks = re.findall(triple_quote_pattern, file_content)

    for block in blocks:
        clean_block = block.strip("\"'").strip()

        if re.search(r"\bSELECT\b|\bFROM\b|\bJOIN\b", clean_block, flags=re.IGNORECASE):
            queries.append(clean_block)

    inline_sql_pattern = r'["\']([^"\']*(?:SELECT|FROM|JOIN)[^"\']*)["\']'
    inline_matches = re.findall(inline_sql_pattern, file_content, flags=re.IGNORECASE)

    for query in inline_matches:
        clean_query = query.strip()
        if clean_query and clean_query not in queries:
            queries.append(clean_query)

    return queries


def collect_table_like_strings(file_content: str) -> List[str]:
    """
    Collects table-like references as evidence.
    Examples:
    project.dataset.table
    `project.dataset.table`
    """

    candidates = set()

    patterns = [
        r"`([A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+)`",
        r"\b([A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+)\b",
    ]

    for pattern in patterns:
        for match in re.findall(pattern, file_content):
            candidates.add(match.strip())

    return sorted(candidates)


def collect_imports(file_content: str) -> List[str]:
    imports = []

    for line in file_content.splitlines():
        stripped = line.strip()

        if stripped.startswith("import ") or stripped.startswith("from "):
            imports.append(stripped)

    return imports[:100]


def detect_probable_platform_from_evidence(file_content: str) -> str:
    lower_content = file_content.lower()

    if "langchain" in lower_content:
        return "LangChain"

    if "crewai" in lower_content or "crew ai" in lower_content:
        return "CrewAI"

    if "autogen" in lower_content:
        return "AutoGen"

    if "llamaindex" in lower_content or "llama_index" in lower_content:
        return "LlamaIndex"

    return "Custom"


def collect_static_evidence(file_path: str, repo_path: str) -> Dict[str, Any]:
    """
    This function only gathers evidence.
    It does not decide final agent datasource mapping.
    """

    file_content = read_file(file_path)
    relative_file = os.path.relpath(file_path, repo_path)

    assignments = extract_python_assignments(file_content)
    class_names = extract_python_class_names(file_content)
    function_names = extract_python_function_names(file_content)
    sql_blocks = collect_sql_blocks(file_content)
    table_like_strings = collect_table_like_strings(file_content)
    imports = collect_imports(file_content)

    evidence = {
        "file_path": relative_file,
        "language": detect_language(file_path),
        "probable_platform": detect_probable_platform_from_evidence(file_content),
        "assignments": assignments,
        "class_names": class_names,
        "function_names": function_names,
        "imports": imports,
        "sql_blocks": sql_blocks,
        "table_like_strings": table_like_strings,
        "code_excerpt": file_content[:12000],
    }

    return evidence


def is_candidate_agent_file(evidence: Dict[str, Any]) -> bool:
    """
    Light filtering only.
    We are not hardcoding datasource detection here.
    We only decide whether this file may be an agent or data-access file.
    """

    text = json.dumps(evidence, default=str).lower()

    candidate_terms = [
        "agent",
        "llm",
        "chatopenai",
        "langchain",
        "crewai",
        "autogen",
        "bigquery",
        "snowflake",
        "postgres",
        "spark.read",
        "datasource",
        "database",
        "query",
        "select",
        "from",
        "table",
        "tool",
        "retriever",
        "vector",
    ]

    return any(term in text for term in candidate_terms)


def call_openai_master_agent(evidence: Dict[str, Any], model: str = "gpt-4o-mini") -> Optional[Dict[str, Any]]:
    """
    DataBuck Master Agent Analyzer.

    Requires:
      OPENAI_API_KEY environment variable.

    If no key is available, this returns None and scanner uses fallback evidence mode.
    """

    print(f"[MASTER_AGENT] Checking OpenAI key for file: {evidence.get('file_path')}")

    api_key = os.getenv("OPENAI_API_KEY")

    if not api_key:
        print("[MASTER_AGENT] OPENAI_API_KEY not found. Using fallback analyzer.")
        return None

    print("[MASTER_AGENT] OPENAI_API_KEY found. Calling LLM now...")

    try:
        from openai import OpenAI
    except Exception:
        print("[MASTER_AGENT] OpenAI package not installed. Install using: pip install openai")
        return None

    client = OpenAI(api_key=api_key)

    system_prompt = """
You are DataBuck Master Agent.

Your job:
Analyze one AI agent source file and identify what the agent is doing.

Return ONLY valid JSON. No markdown. No explanation.

Important:
- Do not execute code.
- Only infer from the provided evidence.
- If something is unknown, return empty string or empty list.
- Identify all datasources used by this agent.
- For BigQuery, extract project, dataset, table, full_table_name, query, and access_type.
- Also identify agent purpose in simple language.
- Give confidence between 0 and 1.
"""

    user_prompt = f"""
Analyze the following agent evidence and return JSON with this schema:

{{
  "is_agent": true,
  "agent_name": "",
  "agent_purpose": "",
  "platform": "",
  "language": "",
  "agent_file": "",
  "datasources": [
    {{
      "datasource_type": "",
      "project": "",
      "dataset": "",
      "table_name": "",
      "full_table_name": "",
      "query": "",
      "access_type": "Read",
      "confidence": 0.0,
      "evidence": ""
    }}
  ],
  "overall_confidence": 0.0
}}

Evidence:
{json.dumps(evidence, indent=2, default=str)}
"""

    try:
        print(f"[MASTER_AGENT] Sending request to model: {model}")

        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0,
        )

        print("[MASTER_AGENT] LLM response received.")

        content = response.choices[0].message.content.strip()

        print("[MASTER_AGENT] Raw LLM response:")
        print(content[:1000])

        if content.startswith("```json"):
            content = content.replace("```json", "").replace("```", "").strip()
        elif content.startswith("```"):
            content = content.replace("```", "").strip()

        parsed_json = json.loads(content)

        print("[MASTER_AGENT] LLM JSON parsed successfully.")

        return parsed_json

    except Exception as error:
        print(f"[MASTER_AGENT] Master Agent failed for {evidence.get('file_path')}: {error}")
        return None


def fallback_master_agent(evidence: Dict[str, Any]) -> Dict[str, Any]:
    """
    Fallback analyzer when no LLM is available.

    This still uses evidence, but the final design keeps this as backup.
    In production, DataBuck Master Agent should be the main analyzer.
    """

    assignments = evidence.get("assignments", {})
    class_names = evidence.get("class_names", [])
    table_like_strings = evidence.get("table_like_strings", [])
    sql_blocks = evidence.get("sql_blocks", [])

    agent_name = assignments.get("agent_name")

    if not agent_name and class_names:
        agent_name = class_names[0]

    if not agent_name:
        agent_name = os.path.basename(evidence.get("file_path", ""))

    datasource_type = ""

    evidence_text = json.dumps(evidence, default=str).lower()

    if "bigquery" in evidence_text:
        datasource_type = "bigquery"
    elif "snowflake" in evidence_text:
        datasource_type = "snowflake"
    elif "postgres" in evidence_text or "psycopg2" in evidence_text:
        datasource_type = "postgresql"
    else:
        datasource_type = "unknown"

    full_table_name = ""

    if table_like_strings:
        full_table_name = table_like_strings[0]

    project = assignments.get("project", "")
    dataset = assignments.get("dataset", "")
    table_name = assignments.get("table_name", "")

    if not full_table_name and project and dataset and table_name:
        full_table_name = f"{project}.{dataset}.{table_name}"

    if full_table_name and full_table_name.count(".") >= 2:
        parts = full_table_name.split(".")
        project = project or parts[0]
        dataset = dataset or parts[1]
        table_name = table_name or parts[2]

    query = sql_blocks[0] if sql_blocks else ""

    datasource = {
        "datasource_type": datasource_type,
        "project": project,
        "dataset": dataset,
        "table_name": table_name,
        "full_table_name": full_table_name,
        "query": query,
        "access_type": "Read",
        "confidence": 0.70 if full_table_name else 0.40,
        "evidence": "Fallback analyzer based on static evidence",
    }

    return {
        "is_agent": True,
        "agent_name": agent_name,
        "agent_purpose": "Agent purpose inferred from code evidence.",
        "platform": evidence.get("probable_platform", "Custom"),
        "language": evidence.get("language", ""),
        "agent_file": evidence.get("file_path", ""),
        "datasources": [datasource] if datasource_type != "unknown" else [],
        "overall_confidence": datasource["confidence"],
    }


def normalize_master_agent_output(master_output: Dict[str, Any], evidence: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Converts Master Agent JSON into flat rows for UI/database.
    One agent can have multiple datasources.
    """

    rows = []

    if not master_output:
        return rows

    if not master_output.get("is_agent", False):
        return rows

    datasources = master_output.get("datasources", [])

    for datasource in datasources:
        datasource_type = datasource.get("datasource_type", "") or ""

        if not datasource_type:
            continue

        row = {
            "agent_name": master_output.get("agent_name", ""),
            "agent_purpose": master_output.get("agent_purpose", ""),
            "agent_file": master_output.get("agent_file", evidence.get("file_path", "")),
            "language": master_output.get("language", evidence.get("language", "")),
            "platform": master_output.get("platform", evidence.get("probable_platform", "")),
            "datasource_type": datasource_type,
            "project": datasource.get("project", ""),
            "dataset": datasource.get("dataset", ""),
            "table_name": datasource.get("table_name", ""),
            "full_table_name": datasource.get("full_table_name", ""),
            "query": datasource.get("query", ""),
            "access_type": datasource.get("access_type", "Read"),
            "confidence": datasource.get("confidence", master_output.get("overall_confidence", 0)),
            "evidence": datasource.get("evidence", ""),
            "scan_status": "Scanned",
            "last_scan": utc_now(),
        }

        rows.append(row)

    return rows


def scan_repository(repo_path: str, use_master_agent: bool = True) -> List[Dict[str, Any]]:
    code_files = discover_code_files(repo_path)
    results = []

    for file_path in code_files:
        evidence = collect_static_evidence(file_path, repo_path)

        if not is_candidate_agent_file(evidence):
            continue

        master_output = None

        if use_master_agent:
            master_output = call_openai_master_agent(evidence)

        if master_output is None:
            master_output = fallback_master_agent(evidence)

        rows = normalize_master_agent_output(master_output, evidence)
        results.extend(rows)

    return results


def write_outputs(results: List[Dict[str, Any]], output_dir: str) -> None:
    os.makedirs(output_dir, exist_ok=True)

    json_path = os.path.join(output_dir, "agent_datasource_mapping.json")
    csv_path = os.path.join(output_dir, "agent_datasource_mapping.csv")

    unique_agents = set(
        r.get("agent_name")
        for r in results
        if r.get("agent_name")
    )

    unique_tables = set(
        r.get("full_table_name")
        for r in results
        if r.get("full_table_name")
    )

    payload = {
        "scan_time": utc_now(),
        "total_agents_detected": len(unique_agents),
        "total_datasource_mappings": len(results),
        "total_tables_detected": len(unique_tables),
        "agents": results,
    }

    with open(json_path, "w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2)

    csv_columns = [
        "agent_name",
        "agent_purpose",
        "agent_file",
        "language",
        "platform",
        "datasource_type",
        "project",
        "dataset",
        "table_name",
        "full_table_name",
        "query",
        "access_type",
        "confidence",
        "scan_status",
        "last_scan",
    ]

    with open(csv_path, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=csv_columns)
        writer.writeheader()

        for row in results:
            writer.writerow({col: row.get(col, "") for col in csv_columns})

    print(f"JSON output: {json_path}")
    print(f"CSV output : {csv_path}")


def print_summary(results: List[Dict[str, Any]]) -> None:
    print("\n========== DataBuck Master Agent Scanner ==========")

    unique_agents = set(
        r.get("agent_name")
        for r in results
        if r.get("agent_name")
    )

    unique_tables = set(
        r.get("full_table_name")
        for r in results
        if r.get("full_table_name")
    )

    print(f"Total agents detected       : {len(unique_agents)}")
    print(f"Total datasource mappings   : {len(results)}")
    print(f"Total tables detected       : {len(unique_tables)}")

    for index, result in enumerate(results, start=1):
        print("\n--------------------------------------------------")
        print(f"Mapping #{index}")
        print(f"Agent Name      : {result.get('agent_name')}")
        print(f"Agent Purpose   : {result.get('agent_purpose')}")
        print(f"Agent File      : {result.get('agent_file')}")
        print(f"Platform        : {result.get('platform')}")
        print(f"Language        : {result.get('language')}")
        print(f"Datasource Type : {result.get('datasource_type')}")
        print(f"Full Table Name : {result.get('full_table_name')}")
        print(f"Access Type     : {result.get('access_type')}")
        print(f"Confidence      : {result.get('confidence')}")
        print(f"Status          : {result.get('scan_status')}")

    print("\n==================================================\n")


def main():
    parser = argparse.ArgumentParser(
        description="DataBuck Master Agent Scanner: scans AI agent repos and detects datasource/table mapping."
    )

    parser.add_argument(
        "--repo-path",
        required=True,
        help="Local repository path to scan",
    )

    parser.add_argument(
        "--output-dir",
        default="scan_output",
        help="Output folder for scanner results",
    )

    parser.add_argument(
        "--disable-master-agent",
        action="store_true",
        help="Disable LLM Master Agent and use fallback evidence analyzer only",
    )

    args = parser.parse_args()

    repo_path = os.path.abspath(args.repo_path)
    output_dir = os.path.abspath(args.output_dir)

    if not os.path.exists(repo_path):
        raise FileNotFoundError(f"Repo path not found: {repo_path}")

    use_master_agent = not args.disable_master_agent

    results = scan_repository(
        repo_path=repo_path,
        use_master_agent=use_master_agent,
    )

    print_summary(results)
    write_outputs(results, output_dir)


if __name__ == "__main__":
    main()
