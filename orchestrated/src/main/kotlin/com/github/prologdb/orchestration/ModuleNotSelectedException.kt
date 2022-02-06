package com.github.prologdb.orchestration

import com.github.prologdb.runtime.PrologRuntimeException

class ModuleNotSelectedException : PrologRuntimeException("No module selected. Select one using :- module(Name).")