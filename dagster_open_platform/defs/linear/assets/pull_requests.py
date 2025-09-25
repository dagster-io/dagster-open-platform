import subprocess
import tempfile

import dagster as dg
from dagster import AssetExecutionContext, EnvVar, Failure, asset
from dagster.components import definitions
from dagster_open_platform.defs.linear.assets.linear_issues import compass_linear_issues
from dagster_open_platform.defs.linear.partitions import linear_issues_partition_def
from dagster_snowflake import SnowflakeResource


@asset(
    group_name="linear",
    partitions_def=linear_issues_partition_def,
    deps=[compass_linear_issues],
    tags={"dagster/kind/linear": "", "dagster/kind/github": ""},
    description="This asset creates a pull request for a Linear issue using Graphite and Claude Code",
)
def compass_pull_requests(context: AssetExecutionContext, snowflake: SnowflakeResource):
    issue_id = context.partition_key
    context.log.info(f"Processing pull request for Linear issue: {issue_id}")

    # Query Linear issue details from fivetran
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()

        query = """
        SELECT
            title,
            description,
            branch_name,
            url
        FROM fivetran.linear.issue
        WHERE id = %s
        AND NOT _fivetran_deleted
        """

        cursor.execute(query, (issue_id,))
        issue_data = cursor.fetchone()

        if not issue_data:
            context.log.warning(f"Issue {issue_id} not found in fivetran data")
            raise Failure(f"Linear issue {issue_id} not found")

        title, description, branch_name, url = issue_data

        #

        # Also log through Dagster logger
        context.log.info(f"Linear Issue Title: {title}")
        context.log.info(f"Linear Issue Description: {description or 'No description provided'}")
        context.log.info(f"Linear Issue Branch Name: {branch_name or 'No branch name provided'}")
        context.log.info(f"Linear Issue URL: {url or 'No URL provided'}")

    with tempfile.TemporaryDirectory() as tmp:
        context.log.info("Using temporary directory %s", tmp)

        # First, clone the repository and install Claude Code
        setup_commands = [
            f"git clone https://x-access-token:{EnvVar('GITHUB_PR_TOKEN').get_value()}@github.com/dagster-io/internal {tmp}/internal-{context.partition_key}",
            f"cd {tmp}/internal-{context.partition_key}/python_modules/dagster-open-platform",
            "npm install -g @anthropic-ai/claude-code",
            "npm install -g @withgraphite/graphite-cli@stable",
            f"gt auth --token {EnvVar('GRAPHITE_TOKEN').get_value()}",
        ]

        context.log.info(f"Running setup commands: {'; '.join(setup_commands)}")

        setup_process = subprocess.run(
            ["bash", "-c", ";".join(setup_commands)], capture_output=True, check=False, text=True
        )

        context.log.info("Setup stdout:")
        context.log.info(setup_process.stdout)

        if setup_process.stderr:
            context.log.warning("Setup stderr:")
            context.log.warning(setup_process.stderr)

        if setup_process.returncode != 0:
            context.log.error(f"Setup failed with return code: {setup_process.returncode}")
            raise Failure(
                description="Failed to setup repository and Claude Code",
                metadata={
                    "stderr": setup_process.stderr,
                    "stdout": setup_process.stdout,
                    "return_code": setup_process.returncode,
                },
            )

        # Create the prompt for Claude Code based on the Linear issue
        prompt = f"""Please implement the following Linear issue:

Title: {title}
Description: {description or "No description provided"}

Please analyze the codebase and make the necessary changes to fulfill this request. Follow these guidelines:
1. Understand the existing code structure and patterns
2. Implement the requested feature or fix
3. Follow the project's coding conventions
4. Update documentation if necessary
5. Create as few new files as possible

Focus on making clean, maintainable changes that integrate well with the existing codebase. Unless otherwise specified,
you should only make changes to .sql files, specifically in the dagster_open_platform_dbt project. Additionally, the focus
of any changes should affect models in the dwh_reporting database."""

        # Use Claude Code's headless mode to process the issue
        repo_path = f"{tmp}/internal-{context.partition_key}/python_modules/dagster-open-platform"

        # Write prompt to a temporary file to avoid shell escaping issues
        prompt_file = f"{tmp}/claude_prompt.txt"
        with open(prompt_file, "w", encoding="utf-8") as f:
            f.write(prompt)

        claude_commands = [
            f"cd {repo_path}",
            f'claude -p "$(cat {prompt_file})" --permission-mode acceptEdits',
        ]

        context.log.info(f"Running Claude Code commands: {'; '.join(claude_commands)}")
        context.log.info("Running Claude Code to implement the Linear issue...")

        claude_process = subprocess.run(
            ["bash", "-c", ";".join(claude_commands)], capture_output=True, check=False, text=True
        )

        # Log Claude Code output
        context.log.info(f"Claude Code return code: {claude_process.returncode}")
        context.log.info("Claude Code stdout:")
        context.log.info(claude_process.stdout)

        if claude_process.stderr:
            context.log.warning("Claude Code stderr:")
            context.log.warning(claude_process.stderr)

        # Check if Claude Code made any changes
        git_status_commands = [f"cd {repo_path}", "git status --porcelain"]

        context.log.info(f"Running git status commands: {'; '.join(git_status_commands)}")

        status_process = subprocess.run(
            ["bash", "-c", ";".join(git_status_commands)],
            capture_output=True,
            check=False,
            text=True,
        )

        context.log.info(f"Git status return code: {status_process.returncode}")
        context.log.info(f"Git status stdout: {status_process.stdout}")

        if status_process.stderr:
            context.log.warning(f"Git status stderr: {status_process.stderr}")

        changes = status_process.stdout.strip()
        if changes:
            context.log.info(f"Claude Code made changes: {changes}")

            # Show the diff for better visibility
            diff_commands = [f"cd {repo_path}", "git diff"]

            context.log.info(f"Running git diff commands: {'; '.join(diff_commands)}")

            diff_process = subprocess.run(
                ["bash", "-c", ";".join(diff_commands)], capture_output=True, check=False, text=True
            )

            context.log.info(f"Git diff return code: {diff_process.returncode}")

            if diff_process.stdout:
                context.log.info("Changes made by Claude Code:")
                context.log.info(diff_process.stdout[:5000])  # Limit to first 5000 chars

            if diff_process.stderr:
                context.log.warning(f"Git diff stderr: {diff_process.stderr}")

            # Use Graphite to create branch and submit PR
            context.log.info("Using Graphite to create branch and submit PR")

            # Use Graphite's AI-powered branch creation and PR submission
            graphite_commands = [
                f"cd {repo_path}",
                "git add -A",
                f"gt create {branch_name} --ai",
                f'gt submit --ai --comment "Link to Linear issue: {url}"',
            ]

            context.log.info(f"Running Graphite commands: {'; '.join(graphite_commands)}")

            graphite_process = subprocess.run(
                ["bash", "-c", ";".join(graphite_commands)],
                capture_output=True,
                check=False,
                text=True,
            )

            context.log.info(f"Graphite return code: {graphite_process.returncode}")
            context.log.info(f"Graphite stdout: {graphite_process.stdout}")

            if graphite_process.stderr:
                context.log.warning(f"Graphite stderr: {graphite_process.stderr}")

            if graphite_process.returncode == 0:
                context.log.info("Successfully created and submitted PR with Graphite")

            else:
                context.log.error("Failed to create/submit PR with Graphite")

        else:
            context.log.info("No changes were made by Claude Code")

        process = claude_process

    if process.returncode != 0:
        stderr = (
            process.stderr if isinstance(process.stderr, str) else process.stderr.decode("utf-8")
        )
        context.log.error(f"Claude Code execution failed: {stderr}")
        raise Failure(
            description="Claude Code execution failure",
            metadata={"stderr": stderr, "stdout": process.stdout},
        )


@definitions
def defs():
    """Linear pull requests asset definitions."""
    return dg.Definitions(
        assets=[compass_pull_requests],
    )
