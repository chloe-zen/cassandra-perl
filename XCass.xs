/* Copyright (c) 2010 Topsy Labs.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of either: the GNU General Public License as published
 * by the Free Software Foundation; or the Artistic License.
 */

#define PERL_NO_GET_CONTEXT /* we want efficiency */
#include <EXTERN.h>
#include <perl.h>
#include <XSUB.h>

#ifndef PERL_VERSION
#    include <patchlevel.h>
#    if !(defined(PERL_VERSION) || (SUBVERSION > 0 && defined(PATCHLEVEL)))
#        include <could_not_find_Perl_patchlevel.h>
#    endif
#    define PERL_REVISION	5
#    define PERL_VERSION	PATCHLEVEL
#    define PERL_SUBVERSION	SUBVERSION
#endif

#ifndef PERL_UNUSED_DECL
#  ifdef HASATTRIBUTE
#    if (defined(__GNUC__) && defined(__cplusplus)) || defined(__INTEL_COMPILER)
#      define PERL_UNUSED_DECL
#    else
#      define PERL_UNUSED_DECL __attribute__((unused))
#    endif
#  else
#    define PERL_UNUSED_DECL
#  endif
#endif

//----------------------------------------------------------------
// Cassandra <-> SVs
//----------------------------------------------------------------

#include <Cassandra.h>
#include <memory>

using namespace std;
using namespace org::apache::cassandra;

static Clock *new_clock(pTHX_ SV *sv) {
    SvGETMAGIC(sv);
    auto_ptr<Clock> c(new Clock);
#if IVSIZE > 4
    if (SvIOK(sv))
        c->timestamp = SvIOK_UV(sv) ? (int64_t)SvUVX(sv) : (int64_t)SvIVX(sv);
    else
#endif
    if (SvNOK(sv))
        c->timestamp = SvNVX(sv);
    else {
        char *e;
        c->timestamp = strtoll(SvPV_nolen(sv), &e, 0);
        if (*e)
            warn("invalid numeric characters in timestamp: %_", sv);
    }
    return c.release();
}


//----------------------------------------------------------------
MODULE=Cassandra	PACKAGE=Cassandra

