# dvx shell integration for bash/zsh
# Install dvx: pipx install dvx
# Add to your ~/.bashrc or ~/.zshrc:
#   eval "$(dvx shell-integration bash)"

# Suffix conventions: c=color, n=no-color, w=ignore-whitespace
#   r=ref (-R, compare to parent), s=refspec (-r)

# Core aliases
alias dx='dvx'
alias dxa='dvx add'
alias dxar='dvx add -R'

# Status
alias dxs='dvx status'
alias dxsv='dvx status -v'
alias dxsy='dvx status -y'
alias dxsj='dvx status --json'

# Diff (content diff)
alias dxd='dvx diff'
alias dxdc='dvx diff -c'
alias dxdn='dvx diff -C'
alias dxdw='dvx diff -w'
alias dxdwc='dvx diff -w -c'

# Diff with ref (r = -R, compare commit to parent)
alias dxdr='dvx diff -R'
alias dxdrc='dvx diff -R -c'
alias dxdrn='dvx diff -R -C'
alias dxdrw='dvx diff -R -w'
alias dxdrwc='dvx diff -R -w -c'

# Diff with refspec (s = -r, explicit refspec)
alias dxds='dvx diff -r'
alias dxdsc='dvx diff -r -c'
alias dxdsn='dvx diff -r -C'
alias dxdsw='dvx diff -r -w'
alias dxdswc='dvx diff -r -w -c'

# Diff summary
alias dxdS='dvx diff -s'

# Cache inspection
alias dxcp='dvx cache path'
alias dxcpr='dvx cache path -r'
alias dxcpR='dvx cache path --remote'
alias dxcm='dvx cache md5'
alias dxcd='dvx cache dir'

# Cat (view cached files)
alias dxct='dvx cat'
alias dxctr='dvx cat -r'

# Checkout
alias dxco='dvx checkout'
alias dxcof='dvx checkout -f'
alias dxcor='dvx checkout -R'
alias dxcorf='dvx checkout -R -f'

# Fetch/Pull/Push
alias dxf='dvx fetch'
alias dxpl='dvx pull'
alias dxps='dvx push'

# GC
alias dxgc='dvx gc'
