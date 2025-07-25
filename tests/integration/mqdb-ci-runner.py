#!/usr/bin/env python3

from collections import defaultdict
import csv
import glob
import json
import logging
import argparse
import os
import random
import shutil
import subprocess
import signal
import time
import zlib  # for crc32
import pathlib
import base64

from integration_test_images import get_docker_env
import zlib

RUNNER_FILE_PATH = os.path.split(os.path.realpath(__file__))[0]
TEST_NAME_FORMAT = f"test_[a-zA-Z0-9]*/"
# TEST_NAME_FORMAT = f"test_[a-zA-Z0-9]*_*[a-zA-Z0-9]*/"

CONTAINER_NAME = "MQDB_integration_tests"
DIND_INTEGRATION_TESTS_IMAGE_NAME = "origin-hub-ai-registry.cn-shanghai.cr.aliyuncs.com/component/mqdb-test-integration-runner"

MAX_RETRY = 3
NUM_WORKERS = 5
SLEEP_BETWEEN_RETRIES = 5
PARALLEL_GROUP_SIZE = 100

TRIES_COUNT = 10
MAX_TIME_SECONDS = 3600

MAX_TIME_IN_SANDBOX = 20 * 60 # 20 minutes
TASK_TIMEOUT = 8 * 60 * 60 # 8 hours

# export CLICKHOUSE_TESTS_SERVER_BIN_PATH=/clickhouse
# export CLICKHOUSE_TESTS_CLIENT_BIN_PATH=/clickhouse
# export CLICKHOUSE_TESTS_BASE_CONFIG_DIR=/clickhouse-config
# export CLICKHOUSE_ODBC_BRIDGE_BINARY_PATH=/clickhouse-odbc-bridge
# export CLICKHOUSE_LIBRARY_BRIDGE_BINARY_PATH=/clickhouse-library-bridge
def init_env():
    os.environ["DOCKER_CLIENT_TIMEOUT"] = "300"
    os.environ["COMPOSE_HTTP_TIMEOUT"] = "600"
    os.environ["PYTHONUNBUFFERED"] = "1"

    # os.environ["CLICKHOUSE_TESTS_SERVER_BIN_PATH"] = "/clickhouse"
    # os.environ["CLICKHOUSE_TESTS_CLIENT_BIN_PATH"] = "/clickhouse"
    # os.environ["CLICKHOUSE_TESTS_BASE_CONFIG_DIR"] = "/clickhouse-config"
    # os.environ["CLICKHOUSE_ODBC_BRIDGE_BINARY_PATH"] = "/clickhouse-odbc-bridge"
    # os.environ["CLICKHOUSE_LIBRARY_BRIDGE_BINARY_PATH"] = "/clickhouse-library-bridge"
    
    
def create_hard_link(src, dst):
    try:
        os.link(src, dst)
    except:
        shutil.copy(src, dst)
    
def create_soft_link(src, dst):
    try:
        os.symlink(src, dst)
    except:
        pass

def copy_dir_or_file(src, dst, max_errors=20):
    error_count = 0
    try:
        if os.path.isdir(src):
            if os.path.exists(dst):
                shutil.rmtree(dst)
            shutil.copytree(src, dst, symlinks=False, ignore_dangling_symlinks=True)
        else:
            if os.path.islink(src):
                try:
                    os.symlink(os.readlink(src), dst)
                except FileExistsError:
                    pass
            else:
                try:
                    shutil.copy(src, dst, follow_symlinks=False)
                except FileNotFoundError:
                    error_count += 1
                    logging.warning(f"File not found: {src}. Skipping... ({error_count}/{max_errors})")
                    if error_count >= max_errors:
                        raise
    except shutil.Error as e:
        logging.warning(f"Copy error: {e}. Continuing with remaining files...")

def stringhash(s):
    return zlib.crc32(s.encode("utf-8"))

def ret_multi_directory(back_num: int, 
        file_path: os.path) -> os.path:
    ret_path = file_path
    for i in range(back_num):
        ret_path = os.path.split(ret_path)[0]
    return ret_path

def replace_build_dir(args, old_path) -> os.path:
    replace_dir = args.build_dir
    if replace_dir == 'build':
        return old_path
    path = pathlib.Path(old_path)
    index = path.parts.index('programs')
    new_prefix_path = os.path.join(get_project_path(),replace_dir)
    new_path = pathlib.Path(new_prefix_path).joinpath(*path.parts[index:])
    logging.info("this is new path {}".format(new_path))
    return new_path
    
        
def check_args_and_update_paths(args):
    if not os.path.isabs(args.clickhouse_root):
        CLICKHOUSE_ROOT = os.path.abspath(args.clickhouse_root)
    else:
        CLICKHOUSE_ROOT = args.clickhouse_root
    
    args.odbc_bridge_binary = replace_build_dir(args, os.path.abspath(args.odbc_bridge_binary))

    args.library_bridge_binary = replace_build_dir(args, os.path.abspath(args.library_bridge_binary))
    
    if not os.path.isabs(args.base_configs_dir):
        args.base_configs_dir = os.path.abspath(args.base_configs_dir)
        
    args.binary = replace_build_dir(args, os.path.abspath(args.binary))
        
    if not os.path.isabs(args.cases_dir):
        args.cases_dir = os.path.abspath(args.cases_dir)
    
    if not os.path.isabs(args.src_dir):
        args.src_dir = os.path.abspath(args.src_dir)
    
    logging.info("base_configs_dir: {}, binary: {}, cases_dir: {} ".format(args.base_configs_dir, args.binary, args.cases_dir))
    for path in [args.binary, args.odbc_bridge_binary, args.library_bridge_binary, args.base_configs_dir, args.cases_dir]:
        if not os.path.exists(path):
            raise Exception("Path {} doesn't exist".format(path))
    
    if (not os.path.exists(os.path.join(args.base_configs_dir, "config.xml"))) and \
            (not os.path.exists(os.path.join(args.base_configs_dir, "config.yaml"))):
        raise Exception("No configs.xml or configs.yaml in {}".format(args.base_configs_dir))
    
    if (not os.path.exists(os.path.join(args.base_configs_dir, "users.xml"))) and \
            (not os.path.exists(os.path.join(args.base_configs_dir, "users.yaml"))):
        raise Exception("No users.xml or users.yaml in {}".format(args.base_configs_dir))


def get_project_path() -> os.path:
    # The default return secondary directory is the project root directory
    return ret_multi_directory(2, RUNNER_FILE_PATH)

def stringTohash(s):
    return zlib.crc32(s.encode("utf-8"))

def get_tests_to_run(test_name_list, 
        hash_total = 1, 
        hash_num = 0):
    test_list = []
    for test_name in test_name_list:
        if stringTohash(test_name) % hash_total == hash_num:
            test_list.append(test_name)
    return test_list

def get_all_test_name(test_path = RUNNER_FILE_PATH, 
        test_regular = TEST_NAME_FORMAT) -> list:
    result = []
    re_file = os.path.join(test_path, test_regular)
    logging.info("search name regular format: {}".format(re_file))
    for dir_name in glob.glob(str(re_file)):
        logging.debug("add all searched test name {}".format(dir_name))
        result.append(str(dir_name).split('/')[-2])
    return result

def docker_kill_handler_handler(signum, frame):
    subprocess.check_call('docker kill $(docker ps -a -q --filter name={name} --format="{{{{.ID}}}}")'.format(name=CONTAINER_NAME), shell=True)
    raise KeyboardInterrupt("Killed by Ctrl+C")

signal.signal(signal.SIGINT, docker_kill_handler_handler)

# print("this is project_path: {}".format(get_project_path()))
# print("this is test_name: {}".format(get_all_test_name()[0]))

def get_test_list(args):
    if len(args.test_list):
        logging.info("Specify test list {}".format(args.test_list))
        return args.test_list
    all_test_list = get_all_test_name()
    all_test_list.sort()
    logging.info("this is test_name: {}".format(all_test_list[0]))
    
    filter_test_list = []
    if args.exclude_test_list_file != "none":
        logging.info("filter test by exclude_test_file {}".format(args.exclude_test_list_file))
        with open(args.exclude_test_list_file) as f:
            for f_line in f.readlines():
                logging.debug("test {} will be filtered out".format(f_line))
                line = f_line.strip('\n')
                filter_test_list.append(line)
    logging.info("need to filter test len {}".format(len(filter_test_list)))
    
    new_test_list = []
    if args.hash_test and args.hash_test_total:
        logging.info("test by hash num and total")
        for i in all_test_list:
            if stringTohash(i) % args.hash_test_total == args.hash_test_num \
                    and i not in filter_test_list:
                new_test_list.append(i)
    else:
        for i in all_test_list:
            if i not in filter_test_list:
                new_test_list.append(i)
    return new_test_list

if __name__ == "__main__":
    # logging.basicConfig(level=logging.INFO, format='%(asctime)s [ %(process)d ] %(levelname)s : %(message)s (%(filename)s:%(lineno)s, %(funcName)s)')
    parser = argparse.ArgumentParser(description="MQDB integration tests runner")
    parser.add_argument(
        "--build-dir",
        default="build",
        help="ck compiled build directory, such as \"build-debug\", \"build-debug-asan\", default build dir \"build\"")
    parser.add_argument(
        "--binary",
        default=os.path.join(get_project_path(), "build/programs/clickhouse"),
        help="Path to clickhouse binary. For example /usr/bin/clickhouse")
    
    parser.add_argument(
        "--odbc-bridge-binary",
        default=os.path.join(get_project_path(), "build/programs/clickhouse-odbc-bridge"),
        help="Path to clickhouse-odbc-bridge binary. Defaults to clickhouse-odbc-bridge in the same dir as clickhouse.")
    
    parser.add_argument(
        "--library-bridge-binary",
        default=os.path.join(get_project_path(), "build/programs/clickhouse-library-bridge"),
        help="Path to clickhouse-library-bridge binary. Defaults to clickhouse-library-bridge in the same dir as clickhouse.")
    
    parser.add_argument(
        "--base-configs-dir",
        default=os.path.join(get_project_path(), "programs/server"),
        help="Path to clickhouse base configs directory with config.xml/users.xml")
    
    parser.add_argument(
        "--cases-dir",
        default=os.path.join(get_project_path(),"tests/integration"),
        help="Path to integration tests cases and configs directory. For example tests/integration in repository")

    parser.add_argument(
        "--utils-dir",
        default=os.path.join(get_project_path(), "utils"),
        help="Path to utils directory in repository. Used to provide some utils for tests")
    
    parser.add_argument(
        "--src-dir",
        default=os.path.join(get_project_path(), "src"),
        help="Path to the 'src' directory in repository. Used to provide schemas (e.g. *.proto) for some tests when those schemas are located in the 'src' directory")
    
    parser.add_argument(
        "--clickhouse-root",
        default=get_project_path(),
        help="Path to repository root folder. Used to take configuration from repository default paths.")
    
    parser.add_argument(
        "--command",
        default='',
        help="Set it to run some other command in container (for example bash)")
    
    parser.add_argument(
        "--disable-net-host",
        action='store_true',
        default=False,
        help="Don't use net host in parent docker container")

    parser.add_argument(
        "--network",
        help="Set network driver for runnner container (defaults to `host`)")
    
    parser.add_argument(
        "--docker-image-version",
        default="latest",
        help="Version of docker image which runner will use to run tests")
    
    parser.add_argument(
        "--docker-compose-images-tags",
        action="append",
        help="Set non-default tags for images used in docker compose recipes(yandex/my_container:my_tag)")

    parser.add_argument(
        "-n", "--parallel",
        action="store",
        dest="parallel",
        help="Parallelism")

    parser.add_argument(
        "-t", "--test_list",
        action="store",
        nargs='+',
        default=[],
        dest="test_list",
        help="List of tests to run")
    
    parser.add_argument(
        "-k", "--keyword_expression",
        action="store",
        dest="keyword_expression",
        help="pytest keyword expression")

    parser.add_argument(
        "--tmpfs",
        action='store_true',
        default=False,
        dest="tmpfs",
        help="Use tmpfs for dockerd files")

    parser.add_argument(
        "--cleanup-containers",
        action='store_true',
        default=False,
        dest="cleanup_containers",
        help="Remove all running containers on test session start")

    parser.add_argument(
        "--dockerd-volume-dir",
        action='store',
        dest="dockerd_volume",
        help="Bind volume to this dir to use for dockerd files")
    
    parser.add_argument(
        "--exclude-test-list-file",
        default="none",
        type=str,
        help="File containing the names of excluded tests")
    
    parser.add_argument(
        "--hash-test",
        action='store_true',
        default=False,
        help="Split test cases using hash")
    
    parser.add_argument(
        "--hash-test-total",
        type=int,
        default=3,
        help="Split test cases into several parts")
    
    parser.add_argument(
        "--hash-test-num",
        type=int,
        default=0,
        help="The number of test cases split using hash")
    
    parser.add_argument(
        "--run-in-ci",
        action='store_true',
        default=False,
        help="Whether the current pipline is running in ci")
    
    parser.add_argument(
        "--runner-image-name",
        default="origin-hub-ai-registry.cn-shanghai.cr.aliyuncs.com/component/mqdb-test-integration-runner",
        # default="mqdb-test-integration-runner",
        help="MQDB Integration tests runner image")
    
    parser.add_argument(
        "--runner-image-version",
        default="3.0.1",
        help="MQDB Integration tests runner version")
    
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="set log level")
    
    parser.add_argument(
        "--run-in-docker",
        default=False,
        action='store_true',
        help="run in docker")
    
    parser.add_argument(
        "--internal-hub-user-b64",
        default="",
        help="internal hub user")
    
    parser.add_argument(
        "--internal-hub-password-b64",
        default="",
        help="internal hub password")
    
    parser.add_argument('pytest_args', nargs='*', help="args for pytest command")
    
    args = parser.parse_args()
    if args.log_level == "INFO":
        level = logging.INFO
    elif args.log_level == "DEBUG":
        level = logging.DEBUG
    elif args.log_level == "WARN":
        level = logging.WARNING
    elif args.log_level == "ERROR":
        level = logging.ERROR
    else:
        level = logging.INFO
    logging.basicConfig(level=level, format='%(asctime)s [ %(process)d ] %(levelname)s : %(message)s (%(filename)s:%(lineno)s, %(funcName)s)')
    check_args_and_update_paths(args)
    
    net = ""
    if args.network:
        net = "--net={}".format(args.network)
    elif not args.disable_net_host:
        net = "--net=host"
    
    env_tags = ""
    
    parallel_args = ""
    if args.parallel:
        parallel_args += "--dist=loadfile"
        parallel_args += " -n {}".format(args.parallel)
    
    if args.docker_compose_images_tags is not None:
        for img_tag in args.docker_compose_images_tags:
            [image, tag] = img_tag.split(":")
            env_tag = get_docker_env(image, tag)
            if env_tag:
                env_tags += env_tag
            else:
                logging.info("Unknown image %s", image)

    env_cleanup = ""
    if args.cleanup_containers:
        env_cleanup = "-e PYTEST_CLEANUP_CONTAINERS=1"
                
    dockerd_internal_volume = ""
    try:
        subprocess.check_call('docker volume create {name}_volume'.format(name=CONTAINER_NAME), shell=True)
    except Exception as ex:
        print("Volume creationg failed, probably it already exists, exception", ex)
    dockerd_internal_volume = "--volume={}_volume:/var/lib/docker".format(CONTAINER_NAME)
    
    if args.keyword_expression:
        args.pytest_args += ['-k', args.keyword_expression]

    test_list = get_test_list(args)
        
    logging.debug("all tests num {}".format(len(test_list)))
    
    if not args.run_in_docker:
        # copy_dir_or_file(args.binary, "/clickhouse")
        # copy_dir_or_file(args.odbc_bridge_binary, "/clickhouse-odbc-bridge")
        # copy_dir_or_file(args.library_bridge_binary, "/clickhouse-library-bridge")
        # copy_dir_or_file(args.base_configs_dir, "/clickhouse-config")
        # copy_dir_or_file(args.cases_dir, "/ClickHouse/tests/integration")
        copy_dir_or_file(args.utils_dir + "/backupview", "/ClickHouse/utils/backupview")
        copy_dir_or_file(args.utils_dir + "/grpc-client/pb2", "/ClickHouse/utils/grpc-client/pb2")
        copy_dir_or_file(args.src_dir + "/Server/grpc_protos", "/ClickHouse/src/Server/grpc_protos")
        init_env()
        os.environ["CLICKHOUSE_TESTS_SERVER_BIN_PATH"] = args.binary
        os.environ["CLICKHOUSE_TESTS_CLIENT_BIN_PATH"] = args.binary
        os.environ["CLICKHOUSE_TESTS_BASE_CONFIG_DIR"] = args.base_configs_dir
        os.environ["CLICKHOUSE_ODBC_BRIDGE_BINARY_PATH"] = args.odbc_bridge_binary
        os.environ["CLICKHOUSE_LIBRARY_BRIDGE_BINARY_PATH"] = args.library_bridge_binary
    
    cmd = "docker run {net} --name {name} --privileged \
        --volume={bin}:/clickhouse \
        --volume={odbc_bridge_bin}:/clickhouse-odbc-bridge \
        --volume={library_bridge_bin}:/clickhouse-library-bridge \
        --volume={base_cfg}:/clickhouse-config \
        --volume={cases_dir}:/ClickHouse/tests/integration \
        --volume={utils_dir}/backupview:/ClickHouse/utils/backupview \
        --volume={utils_dir}/grpc-client/pb2:/ClickHouse/utils/grpc-client/pb2 \
        --volume={src_dir}/Server/grpc_protos:/ClickHouse/src/Server/grpc_protos \
        --volume=/run:/run/host:ro {dockerd_internal_volume} {env_tags} {env_cleanup} \
        -e DOCKER_CLIENT_TIMEOUT=300 -e COMPOSE_HTTP_TIMEOUT=600 -e PYTHONUNBUFFERED=1 \
        -e INTERNAL_HUB_USER={INTERNAL_HUB_USER} -e INTERNAL_HUB_PASSWORD={INTERNAL_HUB_PASSWORD} \
        -e PYTEST_ADDOPTS='{parallel} {opts} {tests_list} -vvv' {img} {command}".format(
            net=net,
            name=CONTAINER_NAME,
            bin=args.binary,
            odbc_bridge_bin=args.odbc_bridge_binary,
            library_bridge_bin=args.library_bridge_binary,
            base_cfg=args.base_configs_dir,
            cases_dir=args.cases_dir,
            utils_dir=args.utils_dir,
            src_dir=args.src_dir,
            dockerd_internal_volume=dockerd_internal_volume,
            env_tags=env_tags,
            env_cleanup=env_cleanup,
            INTERNAL_HUB_USER=base64.b64encode(args.internal_hub_user_b64.encode()).decode(),
            INTERNAL_HUB_PASSWORD=base64.b64encode(args.internal_hub_password_b64.encode()).decode(),
            parallel=parallel_args,
            opts=' '.join(args.pytest_args).replace('\'', '\\\''),
            tests_list=' '.join(test_list),
            img=args.runner_image_name + ":" + args.runner_image_version,
            command=args.command
    )
        
    pytest_cmd = "pytest {parallel} {opts} {tests_list} -vvv".format(
        parallel=parallel_args,
        opts=' '.join(args.pytest_args),
        tests_list=' '.join(test_list)
    )
    
    try:
        if args.run_in_docker:
            logging.info("Trying to kill container {} if it's already running".format(CONTAINER_NAME))
            subprocess.check_call(f'docker stop $(docker ps -a -q --filter name={CONTAINER_NAME} --format="{{{{.ID}}}}")', shell=True)
            subprocess.check_call(f'docker rm $(docker ps -a -q --filter name={CONTAINER_NAME} --format="{{{{.ID}}}}")', shell=True)
            logging.info("Container killed")
    except:
        logging.info("Nothing to kill")
        # print("asda")

    if args.run_in_docker:
        logging.info(("Running pytest container as: '{}'.".format(cmd)))
        subprocess.check_call(cmd, shell=True)

    else:
        logging.info("Running pytest command: '{}'.".format(pytest_cmd))
        subprocess.check_call(pytest_cmd, shell=True)