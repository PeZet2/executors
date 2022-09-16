from subprocess_executor import run_subprocess
from logger import logger
import os


class Git:

    @staticmethod
    def git_checkout_commit(commit_sha: str, project_path: str) -> bool:
        """
        Checks out master branch with the given COMMIT-SHA
        :param commit_sha: Hash of the commit
        :param project_path: Project directory where checkout of the master branch will be performed.
        :return:True or False whether checkout succeeds
        """
        home = os.getcwd()
        os.chdir(project_path)
        logger.debug(f"Changed path to: {project_path}")
        logger.debug(f"Checking out {commit_sha}...")
        is_checked_out = False
        try:
            run_subprocess(f"git checkout -q {commit_sha}")
            run_subprocess(f"git pull -q")
            is_checked_out = True
        except Exception as e:
            logger.error(f"Checkout failed | ERROR -> {e}")
        finally:
            os.chdir(home)
            logger.debug(f"Changed path to: {home}")
            return is_checked_out

    @staticmethod
    def git_fetch_merge_base_commit(commit_branch: str, project_path: str) -> str:
        """
        Returns last master COMMIT-SHA before branching from master into given branch
        :param commit_branch: Name of the branch
        :param project_path: Project directory
        :return: Last master commit-sha
        """
        logger.debug(f"Fetching merge-base master COMMIT_SHA...")
        home = os.getcwd()
        os.chdir(project_path)
        logger.debug(f"Changed path to: {project_path}")

        last_master_commit = ""
        try:
            sub = run_subprocess(f"git merge-base master {commit_branch}")
            last_master_commit = sub.stdout.rstrip("\n")
        finally:
            os.chdir(home)
            logger.debug(f"Changed path to: {home}")
            return last_master_commit

    @staticmethod
    def git_fetch_changed_files_from_branch(project_path: str, branch_name: str, commit_sha: str, *,
                                            refresh_master: bool = False,
                                            master_commit_sha_before_branching: str = None) -> dict:
        """s
        Retrieves files modified inside given branch
        :param project_path: Project directory
        :param branch_name: Name of the current branch
        :param commit_sha: Last COMMIT-SHA from current branch
        :param refresh_master: True or False whether changes are to be compared with master head or with specific commit
        :param master_commit_sha_before_branching: Last commit sha from master before branching
        :return: Files modified inside current branch
        """
        logger.info(f"Fetching changes from branch [{branch_name}]...")
        home = os.getcwd()
        os.chdir(project_path)
        logger.debug(f"Changed path to: {project_path}")

        modified_files = dict()
        try:
            run_subprocess(f"git fetch -q")
            run_subprocess(f"git checkout -q master")
            run_subprocess(f"git pull -q origin master")
            run_subprocess(f"git checkout -q {branch_name}")
            source = "origin/master"
            if master_commit_sha_before_branching and not refresh_master:
                source = master_commit_sha_before_branching
            sub = run_subprocess(f"git diff-tree --no-commit-id --name-status -r {source} -r {commit_sha}")
            for diff in sub.stdout.splitlines():
                diff_type = diff.split("\t")[0]
                diff_file = diff.split("\t")[1]
                modified_files[diff_file] = {}
                modified_files[diff_file]['type'] = diff_type
        finally:
            os.chdir(home)
            logger.debug(f"Changed path to: {home}")

        return modified_files
