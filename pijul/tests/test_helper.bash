setup() {
  # TODO: this is currently only for testing debug builds, but it should be configurable.
  export PATH="$BATS_TEST_DIRNAME/../target/debug:/usr/bin"

  # Make a clean tmpdir for putting repos in (hopefully, it won't be put in an
  # existing pijul repo).
  export PIJUL_REPO_DIR=`mktemp -d`
  cd $PIJUL_REPO_DIR

  # Since the home directory might contain a pijul configuration file, make
  # sure we start with a clean home directory.
  mkdir HOME
  export HOME=`pwd`/HOME
}

teardown() {
  rm -rf "$PIJUL_REPO_DIR"
}

make_two_repos() {
    mkdir "$1"
    pijul init "$1"
    mkdir "$2"
    pijul init "$2"
}

make_random_file() {
    cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 80 | head -n 10 > "$1"
}

append_random() {
    cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 80 | head -n 2 > "$1"
}

assert_success() {
  if [[ "$status" -ne 0 ]]; then
    echo "command failed with exit status $status"
    return 1
  elif [[ "$#" -gt 0 ]]; then
    assert_output "$1"
  fi
}

assert_failure() {
  if [[ "$status" -eq 0 ]]; then
    echo "expected failed exit status"
    return 1
  elif [[ "$#" -gt 0 ]]; then
      assert_output "$1"
  fi
}

assert_output() {
  if [[ ! "$output" =~ $1 ]]; then
    echo "expected: $1"
    echo "actual: $output"
    return 1
  fi
}

assert_files_equal() {
  cmp "$1" "$2"
  if [[ $? -ne 0 ]]; then
    echo "files should be the same"
    echo "first file:"
    cat "$1"
    echo "second file:"
    cat "$2"
    return 1
  fi
}

assert_file_contains() {
  grep --quiet "$2" "$1"
  if [[ $? -ne 0 ]]; then
    echo "file $1 was supposed to contain $2"
    return 1
  fi
}

assert_dirs_equal() {
  diff --exclude=.pijul -u -r "$1" "$2"
  if [[ $? -ne 0 ]]; then
    echo "error comparing directories"
    return 1
  fi
}
