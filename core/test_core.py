# -*- coding: utf-8 -*-
"""
Created on Sun Mar 28 06:39:37 2021

@author: Gary
"""
import core.cas_tools as ct

class Test_cas_tools:
    # make sure the cas_tools work as expected
    def test_is_valid(self):
        assert ct.is_valid_CAS_code('7732-18-5') # standard for water
        assert not ct.is_valid_CAS_code('7732-1-5') # missing digit 
        assert not ct.is_valid_CAS_code('007732-18-5') # padded with zeros
        assert not ct.is_valid_CAS_code('7732-18-4') # wrong check digit
        assert not ct.is_valid_CAS_code('77E2-18-5') # non-digit

    def test_cleanup_cas(self):
        # try to coerce into valid format but don't check if valid
        assert ct.cleanup_cas('7732-18-5') == '7732-18-5'
        assert ct.cleanup_cas('7732-18-4') == '7732-18-4' # not valid but clean
        assert ct.cleanup_cas('a7732-18-5') == '7732-18-5'
        assert ct.cleanup_cas('\n7732-18-5') == '7732-18-5'
        assert ct.cleanup_cas('   7 7 3 2 - 1 8 - 5 ') == '7732-18-5'
        assert ct.cleanup_cas('7732@-1*8-5asdf') == '7732-18-5'
        assert ct.cleanup_cas('07732-18-5') == '7732-18-5'
        assert ct.cleanup_cas('0007732-18-5') == '7732-18-5' # drop leading zeros
        assert ct.cleanup_cas('7732-8-5') == '7732-08-5' # pad middle piece
        assert ct.cleanup_cas('abcd-er-t') == '--' # returns just valid chars
        assert ct.cleanup_cas('1234567897732-18-5') == '1234567897732-18-5'
        assert ct.cleanup_cas('7732-1812-5') == '7732-1812-5'
        assert ct.cleanup_cas('7732-18-54321') == '7732-18-54321'
        


        