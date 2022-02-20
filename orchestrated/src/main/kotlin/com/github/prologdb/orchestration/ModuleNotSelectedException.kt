package com.github.prologdb.orchestration

import com.github.prologdb.runtime.PrologException

class ModuleNotSelectedException : PrologException("No module selected. Select one using :- module(Name).")