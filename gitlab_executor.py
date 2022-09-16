from collections import OrderedDict
from typing import Dict, List
import requests
import urllib3


class GitApi:
    def __init__(self, base_config: dict, token: str = None):
        self.git_api_conf = base_config['GIT_API']
        if token:
            self.git_api_conf["Private-Token"] = token
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def get_response(self, url: str, params=None):
        headers = {"Private-Token": self.git_api_conf["Private-Token"]}
        base_url = self.git_api_conf["URL"]
        response = requests.get(f"{base_url}{url}", headers=headers, verify=False, params=params)
        return response

    def post_response(self, url: str, params=None):
        base_url = self.git_api_conf["URL"]
        response = requests.post(f"{base_url}{url}", verify=False, json=params)
        return response

    def run_pipeline(self, project_id: int, params: dict) -> dict:
        run_pipeline_url = f"/projects/{project_id}/trigger/pipeline"
        return self.post_response(run_pipeline_url, params).json()

    def get_pipeline_status(self, project_id: int, pipeline_id: int) -> dict:
        check_pipeline_url = f"/projects/{project_id}/pipelines/{pipeline_id}"
        response = self.get_response(check_pipeline_url).json()
        return {'status': response.get('status'), 'detailed_status': response.get('detailed_status').get('label')}

    def get_pipeline_jobs_info(self, project_id: int, pipeline_id: int) -> dict:
        check_pipeline_url = f"/projects/{project_id}/pipelines/{pipeline_id}/jobs"
        response = self.get_response(check_pipeline_url).json()
        return {job.get('name'): job.get('id') for job in response}

    def get_commit_modified_files_info(self, project_id: int, commit_sha: str) -> List[Dict]:
        """
        Returns dictionary of changes of the given commit
        :param project_id: Id of current project
        :param commit_sha: Hash of the specific commit from gitlab project
        :return: A list of modified files
        """
        commit_diff_url = f"/projects/{project_id}/repository/commits/{commit_sha}/diff"
        pages_count = int(self.get_response(commit_diff_url).headers["X-Total-Pages"])
        params = {}
        commit_info_list = []
        for page_index in range(1, pages_count + 1):
            params.update({"page": page_index})
            for commit_info in self.get_response(commit_diff_url, params).json():
                commit_info_list.append(commit_info)
        return commit_info_list

    def check_pipeline_jobs(self, project_id: int, pipeline_id: int, job_name: str):
        """
        Checks if job with passed name was successfully executed in pipeline
        """
        pipeline_jobs_url = f"/projects/{project_id}/pipelines/{pipeline_id}/jobs"
        params = {"scope[]": "success"}
        pages_amount = int(self.get_response(pipeline_jobs_url).headers["X-Total-Pages"])
        jobs = OrderedDict()
        for page_index in range(pages_amount):
            params.update({"page": page_index})
            for job in self.get_response(pipeline_jobs_url, params).json():
                jobs[job["name"]] = job["status"]
        return job_name in jobs.keys()

    def get_previous_sha(self, project_id: int, sha: str, ref_name: str, check_prev_job: str,
                         check_prev_pipelines: bool):
        """
        Gets previous sha of pipeline next to given
        """
        pipelines_url = f"/projects/{project_id}/pipelines"
        params = {'ref': ref_name,
                  'per_page': 100}

        # Get total pipeline pages amount
        pages_amount = int(self.get_response(pipelines_url, params).headers["X-Total-Pages"])

        # Scan all pipelines and:
        # If check_prev_job != None, then find latest job with specified name (self.config.check_prev_job)
        # with status "success", and return it's SHA
        # If check_prev_job is None, then find latest pipeline with status "success", and return it's SHA
        prev_pipeline_sha = None
        found = False
        start_analysis = False
        for page_index in range(pages_amount):
            if found:
                break
            params.update({'page': page_index})
            pipelines = self.get_response(pipelines_url, params).json()
            for pipeline in pipelines:
                pipeline = {"id": pipeline["id"],
                            "sha": pipeline["sha"],
                            "status": pipeline["status"]}
                # Start analysis only when pipeline with given SHA found
                if pipeline['sha'] == sha:
                    start_analysis = True
                    continue
                if not start_analysis:
                    continue
                # Stop at first job with selected name ended with success
                if check_prev_job and self.check_pipeline_jobs(project_id, int(pipeline["id"]), check_prev_job):
                    prev_pipeline_sha = pipeline["sha"]
                    found = True
                    break
                # Stop at first pipeline ended with success when check_prev_pipelines is set
                if check_prev_pipelines and pipeline["status"] == "success":
                    prev_pipeline_sha = pipeline["sha"]
                    found = True
                    break

        return prev_pipeline_sha
