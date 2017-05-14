#!/usr/bin/env bats

load test_helper

@test "init forbids nesting" {
    pijul init
    mkdir subdir
    cd subdir
    run pijul init
    assert_failure ^Repository.*already\ exists
}

@test "init another directory" {
    mkdir subdir
    pijul init subdir
    [[ -d subdir/.pijul ]]
}

@test "info in repo" {
    pijul init
    run pijul info

    assert_success
    [[ ${lines[0]} =~ "Current repository root:" ]]
    [[ ${lines[1]} =~ "Current branch:" ]]
}

@test "info out of repo" {
    run pijul info
    assert_failure "error: Not in a repository"
}

@test "add grandchild" {
    pijul init
    mkdir subdir
    touch subdir/file.txt
    pijul add subdir/file.txt
    run pijul record -a -m msg -A "me <me>"
    assert_success "Recorded patch"
}

@test "add only in repo" {
    touch file.txt
    run pijul add file.txt
    assert_failure "error: Not in a repository"
}

@test "add outside repo" {
    mkdir subdir
    cd subdir
    pijul init
    touch ../file.txt
    run pijul add ../file.txt
    assert_failure "error: Invalid path"
}

@test "add from outside repo" {
    mkdir subdir
    touch subdir/file.txt
    pijul init subdir
    pijul add --repository subdir file.txt
    assert_success
}

@test "add same file twice" {
    pijul init
    touch file.txt
    pijul add file.txt
    run pijul add file.txt
    assert_failure "error: Repository error: File already here"
}

@test "nothing to record" {
    pijul init
    run pijul record
    assert_success "Nothing to record"
}

@test "add/remove nothing to record" {
    pijul init
    touch file.txt
    pijul add file.txt
    pijul remove file.txt
    run pijul record
    assert_success "Nothing to record"
}

@test "something to record" {
    pijul init
    touch file.txt
    pijul add file.txt
    run pijul record -a -m msg -A "me <me>"
    assert_success "Recorded patch"
}

@test "no remove without add" {
    pijul init
    touch file.txt
    run pijul remove file.txt
    assert_failure "File file.txt not tracked"
}

@test "add/record/pull/edit/record/pull" {
    make_two_repos a b
    make_random_file a/file.txt
    pijul add --repository a file.txt
    pijul record --repository a -a -m msg -A me
    pijul pull -a a b
    assert_files_equal a/file.txt b/file.txt

    # Pull back the other way, without making any changes
    pijul pull -a b a
    assert_files_equal a/file.txt b/file.txt

    # Now make a change, and pull back
    sed -i '4i add a line' b/file.txt
    sed -i '7D' b/file.txt
    pijul record --repository b -a -m msg -A me
    pijul pull -a b a
    assert_files_equal a/file.txt b/file.txt
}

@test "add/record/pull/delete/record/pull" {
    make_two_repos a b
    make_random_file a/file.txt
    pijul add --repository a file.txt
    pijul record --repository a -a -m msg -A me
    pijul pull -a a b

    pijul remove --repository b file.txt
    pijul record --repository b -a -m msg -A me
    pijul pull -a b a

    [[ ! -f a/file.txt ]]
}

@test "pull empty file" {
    make_two_repos a b
    touch a/file.txt
    pijul add --repository a file.txt
    pijul record --repository a -a -m msg -A me
    pijul pull -a a b
    assert_files_equal a/file.txt b/file.txt
}

@test "move unadded" {
    pijul init
    touch file.txt
    run pijul mv file.txt other.txt
    assert_failure "File file.txt not tracked"
}

@test "move file" {
    pijul init
    make_random_file file.txt
    cp file.txt backup.txt
    pijul add file.txt
    pijul record -a -m msg -A me
    pijul mv file.txt new_file.txt
    pijul record -a -m msg -A me
    assert_files_equal backup.txt new_file.txt
}

@test "move and edit file" {
    mkdir a
    cd a
    pijul init
    make_random_file file.txt
    cp file.txt backup.txt
    pijul add file.txt
    pijul record -a -m msg -A me
    pijul mv file.txt new_file.txt
    sed -i '4c new line' new_file.txt
    sed -i '4c new line' backup.txt
    pijul record -a -m msg -A me
    assert_files_equal backup.txt new_file.txt

    cd ..
    pijul clone a b
    assert_files_equal a/new_file.txt b/new_file.txt
}

@test "pull symmetric" {
    make_two_repos a b
    make_random_file a/toto
    pijul add --repository a toto
    pijul record --repository a -a -m msg -A me

    make_random_file b/titi
    pijul add --repository b titi
    pijul record --repository b -a -m msg -A me

    pijul pull -a a b
    pijul pull -a b a
    assert_files_equal a/toto b/toto
    assert_files_equal a/titi b/titi
}

@test "pull symmetric add/add conflict" {
    make_two_repos a b
    make_random_file a/toto
    pijul add --repository a toto
    pijul record --repository a -a -m msg -A me

    make_random_file b/toto
    pijul add --repository b toto
    pijul record --repository b -a -m msg -A me

    pijul pull -a a b
    pijul pull -a b a
    assert_dirs_equal a b
}

@test "pull symmetric edit/edit conflict" {
    make_two_repos a b
    touch a/toto
    pijul add --repository a toto
    pijul record --repository a -a -m msg -A me
    pijul pull -a a b

    make_random_file a/toto
    make_random_file b/toto
    pijul record --repository a -a -m msg -A me
    pijul record --repository b -a -m msg -A me
    pijul pull -a a b
    pijul pull -a b a
    assert_dirs_equal a b
    assert_file_contains a/toto '>>>>>'
}

@test "pull symmetric edit/edit conflict with context" {
    make_two_repos a b
    make_random_file a/toto
    pijul add --repository a toto
    pijul record --repository a -a -m msg -A me
    pijul pull -a a b

    append_random a/toto
    append_random b/toto
    pijul record --repository a -a -m msg -A me
    pijul record --repository b -a -m msg -A me
    pijul pull -a a b
    pijul pull -a b a
    assert_dirs_equal a b
    assert_file_contains a/toto '>>>>>'
}

