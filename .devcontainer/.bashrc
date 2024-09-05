source /etc/bash_completion.d/git-prompt
export GIT_PS1_SHOWDIRTYSTATE=1
export PS1='\[\e[1;32m\]\u@dev\[\e[m\]:\[\e[1;34m\]\w\[\e[0;33m\]$(__git_ps1 " (%s)")\[\e[1;34m\]$\[\e[m\] '
