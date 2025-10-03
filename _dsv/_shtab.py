import os

def _quote(val):
    return val.replace('\\', '\\\\').replace('"', '\\"').replace('$', '\\$')

class shtab:
    def __getattr__(self, key):
        if os.environ.get('ENABLE_SHTAB') == '1':
            import shtab
            return getattr(shtab, key)

shtab = shtab()

shtab.COMMAND = {
    'zsh': '{_normal}',
    'bash': '''
    _shtab__dsv_pipe_dsv_custom_complete
    _shtab__dsv_original_ifs="$IFS"
    _shtab__dsv_pipe_dsv_custom_complete() {
        local IFS="$_shtab__dsv_original_ifs"
        local _drop=0
        # no idea where the command really begins
        while [[ "$_drop" < "${#COMP_WORDS[@]}" && "${COMP_WORDS[$_drop]}" != pipe ]]; do (( _drop ++ )); done
        (( _drop ++ ))
        while [[ "$_drop" < "${#COMP_WORDS[@]}" && "${COMP_WORDS[$_drop]}" == -* ]]; do (( _drop ++ )); done
        local COMP_WORDS=( "${COMP_WORDS[@]:$_drop}" )
        local COMP_CWORD=$(( COMP_CWORD-_drop ))
        local COMP_LINE="${COMP_WORDS[*]}"
        _command_offset 0
        printf '%s\\n' "${COMPREPLY[@]}"
    }
    '''.strip(),
}
