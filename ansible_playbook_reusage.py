#!/usr/bin/env python3
"""Determine health of Ansible community based on usage of dependencies.

The hypothesis which inspired this module is that the health of an automation
software's community can be approximated, in part, by guaging how well that
community shares and re-uses code.

In a healthy community, code-reuse should be high, indicating that individuals
find it easy to build upon the work of their fellow community members.

In an unhealthy community, code-reuse will be particularly low, indicating that
individuals find the community unwelcoming/unfriendly, find it difficult to
incorporate others' work into their own work, or find that the quality of work
by others in the community is inferior/unfit for use in their own work.
"""

import requests
import os
import contextlib
import shutil
import tempfile
import logging
import tarfile
import textwrap
import pprint

import multiprocessing

try:
    import regex as re
except ImportError:
    # 3rd party "Regex" library is not installed. Rely on default RE module.
    # This is fine. The 'regex' package is just a nice module to have because
    # it releases GIL during RegEx pattern matching. However, the default "re"
    # module will also work.
    import re


ANSIBLE_GALAXY_DOMAIN_NAME = 'galaxy.ansible.com'
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.WARNING)
# Filenames must start with an alphabetic character, but after that can contain
# any number of spaces, alphabetic characters, numbers, underscores, dash, or
# periods; however, under no circumstances may two periods directly follow one
# another, nor may the filename end with a period, space, or dash.
ACCEPTIBLE_BASENAME_PATTERN = \
    r'([a-zA-Z]|([a-z A-Z\d_-]|\.(?!\.)){}[^a-zA-Z\d_]$)'
# We expect the filename in the archive to be a member
# of the set { requirement, requirements }, with an extension being a member of
# the set {yaml, yml}, with case insensitivity.
TARGET_FILENAME_PATTERN = re.compile(
    r'(.+[\\/])?requirements?\.ya?ml$',
    flags=re.IGNORECASE
)


def check_is_playbook_dependency_match(file_uri):
    """Determine if a file URI is a playbook dependency file."""
    if not file_uri or not isinstance(file_uri, str):
        LOGGER.warning(
            'NULL file_uri or non-string value for "%s".',
            file_uri
        )
        file_uri = ""
    # end if

    # Check if it passes the regex check.
    b_is_match = bool(TARGET_FILENAME_PATTERN.match(file_uri))
    # Confirm that it is not part of a Molecule test dirctory.
    b_is_match = b_is_match and not ('molecule' in file_uri)
    # File cannot be in a directory named "tests" or "test"
    b_is_match = b_is_match and not (re.search(r'[\/]tests?[\/]', file_uri))
    return b_is_match


def query_api_url(page_url=None):
    """Query an API URL, and parse it if it's HTML."""
    if not page_url:
        LOGGER.critical('Invalid page_url provided to query_api_url(...)')
        return None
    res = requests.get(page_url)
    retval = res.content
    try:
        retval = res.json()
    except Exception:
        LOGGER.warning('Could not interpret result as JSON.')
    return retval


@contextlib.contextmanager
def tempdir():
    """Make a temporary directory within a WITH-compatible context.

    See Also
    --------
        Source: https://stackoverflow.com/a/33288373/2694511

    """
    dirpath = tempfile.mkdtemp()
    try:
        yield dirpath
    finally:
        shutil.rmtree(dirpath)


def download_resource(url, filepath):
    """Efficiently download file from URL and save to disk."""
    resp_stream = requests.get(url, stream=True)
    resp_stream.raise_for_status()
    with open(filepath, mode='w+b') as fh:
        for file_chunk in resp_stream.iter_content(4096):
            fh.write(file_chunk)
        fh.flush()


def filter_ham_dict_keys(playbook_result=None):
    """Filter a playbook's metadata to only include useful keys."""
    if not playbook_result:
        LOGGER.warning('Received NULL or Empty param in filter_ham_dict_key')
        playbook_result = {}
    HAM_KEY_LIST = frozenset((
        'original_name', 'name',
        'download_count', 'download_url',
        'description', 'id',
        'summary_fields'
    ))
    return {
        key: val
        for key, val in playbook_result.items()
        if key in HAM_KEY_LIST
    }


def _format_url_get_most_downloaded_playbooks(playbook_count):
    api_repo_query_uri = '/api/v1/repositories/'
    # Example: &page_size=100
    playbook_count_param = '&page_size={}'.format(playbook_count)
    most_downloaded_orderby_param = '&order_by=-download_count'
    s_api_url = 'https://{}{}?format=json{}{}'.format(
        ANSIBLE_GALAXY_DOMAIN_NAME,
        api_repo_query_uri,
        most_downloaded_orderby_param,
        playbook_count_param
    )
    return s_api_url


def get_info_most_downloaded_playbooks(playbook_count=100):
    """Get metadata about the playbook_count most downloaded playbooks."""
    s_api_url = _format_url_get_most_downloaded_playbooks(playbook_count)
    query_response = query_api_url(s_api_url)
    retvals = [
        filter_ham_dict_keys(r)
        for r in query_response['results']
    ]
    return retvals


def extract_all_tar_gzip(tgz_path):
    """Extract all members of an archive."""
    das_archive = tarfile.open(tgz_path, mode='r:*')
    das_archive.extractall()
    das_archive.close()


def convert_needle_to_regex_pattern(needle):
    """Return needle as a regex Pattern object."""
    if not callable(getattr(needle, 'match', None)):
        needle = re.compile(needle, flags=re.IGNORECASE)
    return needle


def _generate_matches_in_list(haystack, needle):
    """Yield match for a needle within a haystack."""
    needle_handler = None
    if not callable(needle):
        needle = convert_needle_to_regex_pattern(needle)
        needle_handler = needle.match
    else:
        needle_handler = needle
    for hay in haystack:
        if needle_handler(hay):
            yield hay


def format_output_for_playbook_hit(playbook_info):
    """Provide function which can embed output related to a playbook."""
    # Remove out the 'versions' attribute from the summary_fields, if present,
    # as that array becomes very long in the final output.
    summary_fields = playbook_info.get('summary_fields', None)
    if summary_fields and ('versions' in summary_fields):
        del summary_fields['versions']
    # Format the strings so the playbook information using Pretty Print module
    s_out_info = pprint.pformat(playbook_info)
    # Embed the formatted playbook info into an output string
    # which indents the playbook info 4-spaces.
    s_out_info = "\nFound Match:\n{}\n\n".format(
        re.sub('^', ' '*4, s_out_info)
    )
    return s_out_info


def get_matches_in_list(haystack, needle):
    """Match a needle against members of a haystack."""
    return tuple(_generate_matches_in_list(haystack, needle))


def check_does_archive_contain_filename_like(archive, re_matcher):
    """Compare an archive's filenames against an acceptable RegEx pattern."""
    return bool(get_matches_in_list(archive.getnames(), re_matcher))


def decode_lines_on_demand(lines=None, encoding='utf-8'):
    """Decode lines on the fly."""
    if not lines:
        lines = []
    for l in lines:
        if callable(getattr(l, 'decode', None)):
            yield l.decode(encoding, 'ignore')
        else:
            yield l


def _filter_useless_yaml_lines(content_lines):
    """Filter out lines that are useless in YAML."""
    # Lines are only useful if they are not empty (blank or only whitespace),
    # are not comments, and do not begin with '---' (which would indicate a
    # separator between two different documents).
    if callable(getattr(content_lines, 'decode', None)):
        # We probably have a bytes object, which we should transform to a
        # UTF-8 string before we try matching.
        content_lines = content_lines.decode('UTF-8', 'ignore')
    return tuple(filter(
        lambda line_content: (
            line_content
            and line_content.strip()
            and not line_content.strip().startswith('#')
            and not line_content.strip().startswith('-'*3)
        ),
        decode_lines_on_demand(content_lines)
    ))


def post_process_file_checks(archive_handle, filenames):
    """Read requirment file matches to determine number of valid lines.

    Parameters
    ----------
    archive_handle : TarFile
        Handle to a TarFile object obtained via tarfile.open(...)
    filenames : list of str
        List containing string representations of filenames.

    """
    valid_lines = {}
    valid_line_count = 0
    for filename in filenames:
        file_reader = archive_handle.extractfile(filename)
        if not file_reader:
            LOGGER.warning(
                "Returned filereader for filename '%s' was NULL",
                filename
            )
        else:
            # Read file contents
            content = file_reader.read()
            all_lines = content.splitlines()
            good_lines = _filter_useless_yaml_lines(all_lines)
            if good_lines:
                valid_line_count += len(good_lines)
                valid_lines[filename] = good_lines
    # end for filenames loop
    b_had_valid_lines = valid_line_count > 0
    return (b_had_valid_lines, valid_lines, valid_line_count)


def check_playbook_for_dependencies(playbook_info):
    """Download, extract & check for requirements file."""
    summary_fields = playbook_info.get('summary_fields', {})
    versions_wrapper = summary_fields.get('versions', list(dict()))
    if len(versions_wrapper) <= 0:
        # LOGGER.warning(
        #     (
        #         'Received very strange "versions" info for playbook.\n'
        #         'Type: %s\n'
        #         'Contents:\n\t%s\n'
        #         'Playbook info:\n\t%s\n'
        #     ),
        #     type(versions_wrapper),
        #     re.sub('^', ' '*4, pprint.pformat(versions_wrapper)),
        #     re.sub('^', ' '*4, pprint.pformat(playbook_info))
        # )
        versions_wrapper = [dict()]
    latest_version_data = dict()
    try:
        latest_version_data = versions_wrapper[0]
    except IndexError:
        LOGGER.critical('Cannot get version data from versions wrapper!')
        LOGGER.warning('\nInfo:\n\t%s\n\n', pprint.pformat(playbook_info))
    latest_version = latest_version_data.get('version', None)
    download_url = latest_version_data.get('download_url', None)
    # Save name begins with the original repo name, but if that is missing for
    # some reason, the name of the playbook as it appears in Ansible Galaxy
    # will be used instead.
    save_name = playbook_info.get(
        'original_name',
        playbook_info.get('name', '')
    )
    if latest_version:
        # Cool. It had the `summary_fields`.`versions` list.
        LOGGER.info(
            'Latest version of "%s" playbook is "%s"',
            playbook_info.get('original_name', playbook_info.get('name', '')),
            latest_version
        )
        save_name = "{}-{}.tar.gz".format(
            save_name, latest_version
        )
    else:
        # It did not have the desired field, so we could not get the version
        # number.
        LOGGER.info(
            'Could not find versions list for playbook: "%s"',
            playbook_info.get('original_name', playbook_info.get('name', '')),
        )
        save_name = "{}-{}.tar.gz".format(
            save_name, 'latest'
        )
    if not download_url:
        download_url = playbook_info.get('download_url')

    b_playbook_has_requirements = False
    post_process_result = []
    # Download into temporary directory and check requirements file.
    with tempdir() as tmp_dir:
        full_save_path = os.path.join(tmp_dir, save_name)
        download_resource(download_url, full_save_path)
        with tarfile.open(full_save_path, 'r:*') as playbook_tgz:
            a_requirement_files = get_matches_in_list(
                playbook_tgz.getnames(),
                check_is_playbook_dependency_match
            )
            b_has_requirements_file = bool(a_requirement_files)
            if b_has_requirements_file:
                # Post-process requirements file.
                post_process_result = post_process_file_checks(
                    playbook_tgz,
                    a_requirement_files
                )
                b_playbook_has_requirements = post_process_result[0]
                # s_out_info = format_output_for_playbook_hit(playbook_info)
                # print(s_out_info)
    # end with TMP_DIR
    return (b_playbook_has_requirements, playbook_info, post_process_result)


def orchestrate_playbook_audit(check_count=100):
    """Orchestrate audit of most popular playbooks."""
    # Get 100 most popular playbooks, and filter out any potential duds.
    playbooks = tuple(filter(
        lambda x: bool(x),
        get_info_most_downloaded_playbooks(check_count)
    ))
    outs = []
    with multiprocessing.Pool(processes=10) as poolparty:
        pool_results = poolparty.map(
            check_playbook_for_dependencies,
            playbooks
        )
        for check_result in pool_results:
            if check_result[0]:
                # print(format_output_for_playbook_hit(check_result[1]))
                outs.append(check_result)

        # print('All mapped and at the end of WITH multiprocessing.Pool block')
    # print('Cleanup after poolparty has concluded.')
    return outs


def main():
    """Call download and analysis method, and then summarize results."""
    check_count = 100
    playbooks_with_requirements = orchestrate_playbook_audit(check_count)
    OFFSET_OF_PLAYBOOK_INFO = 1
    OFFSET_OF_REQUIREMENTS_INFO = 2
    requirements_count = 0
    for pbwr in playbooks_with_requirements:
        playbook_output = format_output_for_playbook_hit(
            pbwr[OFFSET_OF_PLAYBOOK_INFO]
        )
        dependency_info = format_output_for_playbook_hit(
            pbwr[OFFSET_OF_REQUIREMENTS_INFO][1]
        )
        pb_req_count = pbwr[OFFSET_OF_REQUIREMENTS_INFO][2]
        requirements_count += pb_req_count
        print(
            (
                "--------------------\n"
                "Playbook that uses dependencies:\n    {}\n"
                "Dependencies Called by this playbook:\n    {}"
            ).format(playbook_output, dependency_info)
        )
    hit_count = len(playbooks_with_requirements)
    # Format the header and description
    a_summary_header = (
        "SUMMARY",
        "Ansible Galaxy Resource Dependency (requirements) Utilization"
    )
    # Get the length of the longest string from the summary headers, then
    # ensure that the length is at least 80 chars (the length of our textwrap).
    pad_length_for_summary = max(len(max(a_summary_header, key=len)), 80)
    s_summary_header = '\n{}\n'.format(
        '\n'.join(
            (s.center(pad_length_for_summary) for s in a_summary_header)
        )
    )
    # Format the summary body.
    s_summary_body = (
        "Overall, {hitcount}/{totalchecked} ({positivepercent:.2f}%) of the "
        "most downloaded playbooks rely on one or more other playbooks. "
        "Across the most downloaded playbooks, a total of {reqcount} "
        "requirements were observed."
    ).format(
        hitcount=hit_count,
        totalchecked=check_count,
        positivepercent=(100*hit_count/check_count),
        reqcount=requirements_count,
    )
    s_summary_body = textwrap.fill(s_summary_body, width=80)
    s_summary_content = "{}\n{}\n".format(
        s_summary_header,
        s_summary_body
    )
    # Print summary content
    print(s_summary_content)


if __name__ == '__main__':
    # Script called directly.
    main()
