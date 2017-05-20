#!/usr/bin/env bats

load test_helper

@test "remember author in a repo" {
    pijul init
    touch file.txt
    pijul add file.txt
    pijul record -a -m msg -A AUTHOR
    echo foo > file.txt
    pijul record -a -m msg
    run pijul changes
    assert_success AUTHOR.*AUTHOR
    [[ -f $HOME/.config/pijul/global.toml ]]
    [[ ! -f .pijul/meta.toml ]]
}

@test "remember authors across repos" {
    make_two_repos a b
    touch a/file.txt
    pijul add --repository a file.txt
    pijul record -a --repository a -m msg -A AUTHOR

    touch b/file.txt
    pijul add --repository b file.txt
    pijul record -a --repository b -m msg
    run pijul changes --repository b
    assert_success AUTHOR
    [[ -f $HOME/.config/pijul/global.toml ]]
}

@test "remember author in a repo even if HOME is unreadable" {
    pijul init
    touch file.txt
    pijul add file.txt
    chmod ugo-w HOME
    run pijul record -a -m msg -A AUTHOR
    assert_success "Warning: failed to save default authors in system-wide configuration"

    echo foo > file.txt
    pijul record -a -m msg
    run pijul changes
    assert_success AUTHOR.*AUTHOR
    [[ -f .pijul/meta.toml ]]
}
