from logger import logger
import subprocess


def run_subprocess(command: str) -> subprocess:
    """
    Runs shell command
    :param command: Shell command to run
    :return: Subprocess object for the given command. It consists of two outputs stdout and stderr
    """
    logger.debug(f"Executing command: '{command}'")

    sub = subprocess.run(command,
                         shell=True, text=True,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if sub.stderr or sub.returncode != 0:
        logger.error(f"Command '{sub.args}' returned non-zero exit status {sub.returncode}. "
                     f"Error message: {sub.stderr}")
        exit(sub.returncode or 999)
    for line in sub.stdout.splitlines():
        logger.debug(line)
    return sub
