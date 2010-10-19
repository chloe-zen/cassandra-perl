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


// Cassandra headers

#include <Cassandra.h>
#include <transport/TSocket.h>
#include <transport/TBufferTransports.h>
#include <protocol/TBinaryProtocol.h>
#include <memory>

using namespace std;
using namespace org::apache::cassandra;
using namespace apache::thrift;  // assuming no conflict


//----------------------------------------------------------------
// How to turn C++ exceptions into calls of Carp::croak
//----------------------------------------------------------------

#define TRY \
    do {                   \
      bool kaboom = false; \
      char croak_msg[256]; \
      try

#define CATCH   CATCH_WITH()
#define CATCH_WITH(RECOVERY) \
      catch (const std::exception &e) {                         \
        kaboom = true;                                          \
        snprintf(croak_msg, sizeof croak_msg, "%s", e.what());  \
      }                                                         \
      catch (...) {                                             \
        kaboom = true;                                          \
        strcpy(croak_msg, "Unknown exception");                 \
      }                                                         \
      if (kaboom) {                                             \
        RECOVERY;                                               \
        ENTER;                                                  \
        SAVETMPS;                                               \
        PUSHMARK(SP);                                           \
        XPUSHs(sv_2mortal(newSVpv(croak_msg, 0)));              \
        PUTBACK;                                                \
        call_pv("Carp::confess", G_DISCARD);                    \
        SPAGAIN;                                                \
        FREETMPS;                                               \
        LEAVE;                                                  \
        Perl_croak(aTHX_ "BUG: confess() did not die! %s", croak_msg); \
      }                                                         \
    } while (0)


//----------------------------------------------------------------
// Cassandra <-> SVs
//----------------------------------------------------------------


// safe int64_t, with maximum value preservation

#define assign_int64(out_i,sv)  pl_assign_int64(aTHX_ out_i,sv)
#define sv_setint64(sv,i)       pl_sv_setint64( aTHX_ sv,i)
#define newSVint64(i)           pl_newSVint64( aTHX_ i)

static void pl_assign_int64(pTHX_ int64_t &out_i, SV *sv) {
    SvGETMAGIC(sv);
#if IVSIZE > 4
    if (SvIOK(sv))
        out_i = SvIOK_UV(sv) ? (int64_t)SvUVX(sv) : (int64_t)SvIVX(sv);
    else
#endif
    if (SvNOK(sv))
        out_i = SvNVX(sv);
    else {
        char *e;
        out_i = strtoll(SvPV_nolen(sv), &e, 0);
        if (*e)
            warn("invalid numeric characters in int64: %_", sv);
    }
}

static void pl_sv_setint64(pTHX_ SV *sv, int64_t i) {
#if IVSIZE > 4
    sv_setiv_mg(sv, i);
#else
    sv_setnv_mg(sv, (NV)i);
#endif
}

static SV *pl_newSVint64(pTHX_ int64_t i) {
#if IVSIZE > 4
    return newSViv(i);
#else
    return newSVnv((NV)i);
#endif
}

// safe arrays

static SV *pl_av_fetch_safe(pTHX_ AV *av, I32 index) {
    SV **svp = av_fetch(av, index, 0);
    return svp ? *svp : &PL_sv_undef;
}
#define av_fetch_safe(av,index)  pl_av_fetch_safe(aTHX_ av,index)

// safe hashes

static SV *pl_hv_fetch_safe(pTHX_ HV *hv, const char *key, I32 klen) {
    SV **svp = hv_fetch(hv, key, klen, 0);
    return svp ? *svp : &PL_sv_undef;
}
#define hv_fetch_safe(hv,key,klen)              pl_hv_fetch_safe(aTHX_ hv,key,klen)
#define hv_fetchs_safe(hv,key)      ("x"key"x", pl_hv_fetch_safe(aTHX_ hv,key,sizeof(key)-1))

// safe strings

static string pl_make_string(pTHX_ SV *sv) {
    STRLEN tn;
    const char *tp = SvPV(sv, tn);
    string(tp, tn);
}
static void pl_assign_string(pTHX_ string &s, SV *sv) {
    STRLEN tn;
    const char *tp = SvPV(sv, tn);
    s.assign(tp, tn);
}
static void pl_assign_string_maybe(pTHX_ string &s, bool &isset, SV *sv) {
    SvGETMAGIC(sv);
    if ((isset = SvOK(sv))) {
        STRLEN tn;
        const char *tp = SvPV_nomg(sv, tn);
        s.assign(tp, tn);
    }
}
#define make_string(sv)               pl_make_string(aTHX_ sv)
#define assign_string(s,sv)           pl_assign_string(aTHX_ s,sv)
#define assign_string_maybe(s,is,sv)  pl_assign_string_maybe(aTHX_ s,is,sv)

#define newSVstring(s)  newSVpvn((s).data(), (s).size())

// safe strings in hashes

static void assign_map(pTHX_ map<string,string> &m, SV *sv) {
    if (SvTYPE(sv) != SVt_PVHV)
        croak("expected hash to initialize map");

    HV *hv = (HV *)sv;
    hv_iterinit(hv);

    SV *valsv;
    char *kp;
    I32 klen;
    while ((valsv = hv_iternextsv(hv, &kp, &klen)) && klen >= 0)
        m[string(kp, klen)] = make_string(valsv);
}


//----------------------------------------------------------------
// Generic assignment of SV* to Thrift objects 
// The single function name "pl_assign_thrift" appears in the typemap
//

#define assign_thrift(t,sv)  pl_assign_thrift(aTHX_ t,sv)
#define thrift_newsv(t)      pl_thrift_newsv(aTHX_ t)


// AuthenticationRequest

static void pl_assign_thrift(pTHX_ AuthenticationRequest &ar, SV *src_sv) {
    assign_map(aTHX_ ar.credentials, src_sv);
}


// ColumnPath and ColumnParent

static void pl_assign_cp(pTHX_ string *fam,
                         string *sup, bool *isup,
                         string *col, bool *icol,
                         SV *src_sv) {
    bool has_family = false;
    switch (SvTYPE(src_sv)) {
      case SVt_PVAV: {
        AV *src_av = (AV*)src_sv;
        I32 ix = 0;
                 assign_string_maybe(*fam, has_family, av_fetch_safe(src_av, ix++));
        if (sup) assign_string_maybe(*sup, *isup,      av_fetch_safe(src_av, ix++));
        if (col) assign_string_maybe(*col, *icol,      av_fetch_safe(src_av, ix++));
        break;
      }

      case SVt_PVHV: {
        HV *src_hv = (HV*)src_sv;
                 assign_string_maybe(*fam, has_family, hv_fetchs_safe(src_hv, "family"));
        if (sup) assign_string_maybe(*sup, *isup,      hv_fetchs_safe(src_hv, "super_column"));
        if (col) assign_string_maybe(*col, *icol,      hv_fetchs_safe(src_hv, "column"));
        break;
      }

      default:
        assign_string_maybe(*fam, has_family, src_sv);
        break;
    }
    if (!has_family)
        throw col ? "Missing column family in column path"
                  : "Missing column family in column parent";
}

static void pl_assign_thrift(pTHX_ ColumnPath &path, SV *src_sv) {
    pl_assign_cp(aTHX_ &path.column_family,
                       &path.super_column,   &path.__isset.super_column,
                       &path.column,         &path.__isset.column,
                       src_sv);
}

static void pl_assign_thrift(pTHX_ ColumnParent &parent, SV *src_sv) {
    pl_assign_cp(aTHX_ &parent.column_family,
                       &parent.super_column,   &parent.__isset.super_column,
                       NULL,                   NULL,
                       src_sv);
}


// Column Value (no name): array of value, timestamp, and maybe ttl

#define assign_colval(col,src_sv)  pl_assign_colval(aTHX_ col,src_sv)
#define colval_newsv(col)          pl_colval_newsv(aTHX_ col)

static void pl_assign_colval(pTHX_ Column &col, SV *src_sv) {
    SV **svp;
    switch (SvTYPE(src_sv)) {
      case SVt_PVAV: {
        AV *av = (AV*)src_sv;
        I32 ix = 0;
        assign_string(col.value, av_fetch_safe(av, ix++));
        if ((svp = av_fetch(av, ix++, 0))) {
            col.ttl = SvIV(*svp);
            col.__isset.ttl = true;
        }
        break;
      }

      case SVt_PVHV: {
        HV *src_hv = (HV*)src_sv;
        if ((svp = hv_fetchs(src_hv, "value", 0)))
            assign_string(col.value, *svp);
        if ((svp = hv_fetchs(src_hv, "ttl", 0))) {
            col.ttl = SvIV(*svp);
            col.__isset.ttl = true;
        }
        break;
      }

      default:
        assign_string(col.value, src_sv);
        break;
    }
}

static SV *pl_colval_newsv(pTHX_ const Column &col) {
    AV *av = newAV();
    av_push(av, newSVstring(col.value));
    av_push(av, newSVint64(col.timestamp));
    if (col.__isset.ttl)
        av_push(av, newSViv(col.ttl));
}

// SuperColumn value (no name): vector<Column> as HV

static void pl_assign_thrift(pTHX_ vector<Column> &cols, SV *src_sv) {
    // TODO
}

// forward declaration:
static void _hv_store_thrift(pTHX_ HV *hv, const Column &col);

static SV *pl_thrift_newsv(pTHX_ const vector<Column> &cols) {
    HV *cols_hv = newHV();
    try {
        for (size_t i = 0; i < cols.size(); ++i)
            _hv_store_thrift(aTHX_ cols_hv, cols[i]);
    }
    catch (...) {
        SvREFCNT_dec(cols_hv);
        throw;
    }
    return (SV*)cols_hv;
}


// Column and SuperColumn as hash with name key are so similar,
//   it seems best to combine them

static SV *_get_single_hash(pTHX_ SV *hash_sv, string &out_key) {
    if (SvTYPE(hash_sv) != SVt_PVHV)
        throw "expected a hash";
    HV *hv = (HV *)hash_sv;
    hv_iterinit(hv);

    SV *valsv;
    char *kp;
    I32 klen;
    if (!(valsv = hv_iternextsv(hv, &kp, &klen)))
        throw "expected a hash with one key";
    if (hv_iternext(hv))
        throw "expected a hash with one key";
    out_key.assign(kp, klen);
    return valsv;
}
static void pl_assign_thrift(pTHX_ Column &col, SV *src_sv) {
    assign_colval(col,        _get_single_hash(aTHX_ src_sv, col.name));
}
static void pl_assign_thrift(pTHX_ SuperColumn &sc, SV *src_sv) {
    assign_thrift(sc.columns, _get_single_hash(aTHX_ src_sv, sc.name));
}

static void _hv_store_thrift(pTHX_ HV *hv, const Column &col) {
    hv_store(hv, col.name.data(), col.name.size(), colval_newsv(col),  0);
}
static void _hv_store_thrift(pTHX_ HV *hv, const SuperColumn &sc) {
    hv_store(hv, sc.name.data(),  sc.name.size(),  thrift_newsv(sc.columns), 0);
}
template <class T>
static HV *_thrift_newhv(pTHX_ const T &t) {
    HV *hv = newHV();
    try {
        _hv_store_thrift(aTHX_ hv, t);
    }
    catch (...) {
        SvREFCNT_dec(hv);
        throw;
    }
    return hv;
}
static SV *pl_thrift_newsv(pTHX_ const Column &col) {
    (SV*)_thrift_newhv(aTHX_ col);
}
static SV *pl_thrift_newsv(pTHX_ const SuperColumn &sc) {
    (SV*)_thrift_newhv(aTHX_ sc);
}


// ColumnOrSuperColumn

static SV *pl_thrift_newsv(pTHX_ const ColumnOrSuperColumn &cos) {
    if (cos.__isset.super_column)
        return thrift_newsv(cos.super_column);
    else if (cos.__isset.column)
        return thrift_newsv(cos.column);
    else
        return &PL_sv_undef;
}


// SliceRange

static void pl_assign_slicerange_maybe(pTHX_ SliceRange &out_sr, bool &isset, HV *src_hv) {
    SV **svp;
    if ((svp = hv_fetchs(src_hv, "start", 0))) {
        assign_string(out_sr.start, *svp);
        isset = true;
    }
    if ((svp = hv_fetchs(src_hv, "finish", 0))) {
        assign_string(out_sr.finish, *svp);
        isset = true;
    }
    if ((svp = hv_fetchs(src_hv, "reversed", 0))) {
        SvGETMAGIC(*svp);
        out_sr.reversed = SvTRUE(*svp); 
        isset = true;
    }
    if ((svp = hv_fetchs(src_hv, "count", 0))) {
        out_sr.start = SvIV(*svp);
        isset = true;
    }
}

static void pl_assign_thrift(pTHX_ SliceRange &out_sr, SV *src_sv) {
    if (SvTYPE(src_sv) != SVt_PVHV) {
        SvGETMAGIC(src_sv);
        if (!SvOK(src_sv))
            return;          // it's empty, so no range
        throw "expected hash for slice range";
    }
    bool ignored_isset;
    pl_assign_slicerange_maybe(aTHX_ out_sr, ignored_isset, (HV*)src_sv);
}


// SlicePredicate

static void pl_assign_thrift(pTHX_ SlicePredicate &out_sp, SV *src_sv) {
    if (SvTYPE(src_sv) != SVt_PVHV) {
        SvGETMAGIC(src_sv);
        if (!SvOK(src_sv))
            return;          // it's empty, so no predicate
        throw "expected hash for slice predicate";
    }

    HV *hv = (HV*)src_sv;
    SV **svp;

    pl_assign_slicerange_maybe(aTHX_ out_sp.slice_range, out_sp.__isset.slice_range, hv);

    if ((svp = hv_fetchs(hv, "columns", 0))) {
        if (SvTYPE(*svp) != SVt_PVAV)
            throw "expected array for slice predicate 'columns'";
        out_sp.__isset.column_names = true;
        AV *av = (AV *)*svp;
        I32 amax = av_len(av);
        for (I32 i = 0; i <= amax; ++i)
            out_sp.column_names.push_back(make_string(av_fetch_safe(av, i)));
    }
}


// a vector of any thrift type

template <class T>
SV *pl_thrift_newsv(pTHX_ const vector<T> &v) {
    AV *av = newAV();
    try {
        for (size_t i = 0; i < v.size(); ++i)
            av_push(av, thrift_newsv(v[i]));
    }
    catch (...) {
        SvREFCNT_dec(av);
        throw;
    }
    return (SV*)av;
}


//----------------------------------------------------------------
// transport helpers
//

static transport::TSocket *client_socket_transport(CassandraClient *cl) {
    transport::TTransport *t = dynamic_cast<transport::TTransport *>(cl->getOutputProtocol()->getTransport().get());
    transport::TUnderlyingTransport *u;
    while ((u = dynamic_cast<transport::TUnderlyingTransport *>(t)))
        t = u;
    return dynamic_cast<transport::TSocket *>(t);
}


//================================================================

typedef CassandraClient XClient;


MODULE=Cassandra	PACKAGE=Cassandra

static XClient *
XClient::_new(I32 string_limit = 0, I32 container_limit = 0)
  CODE:
    boost::shared_ptr<transport::TSocket>
        trans(new transport::TSocket);

    boost::shared_ptr<protocol::TProtocol>
        proto(new protocol::TBinaryProtocol(trans,
                                            string_limit,
                                            container_limit,
                                            true,   // strict_read
                                            true)); // strict_write

    RETVAL = new XClient(proto);
  OUTPUT:
    RETVAL


void
XClient::connect(string host, int port)
  CODE:
    transport::TSocket *tsock = client_socket_transport(THIS);
    tsock->close();
    tsock->setHost(host);
    tsock->setPort(port);

void
XClient::disconnect()
  CODE:
    transport::TSocket *tsock = client_socket_transport(THIS);
    tsock->close();


# virtual AccessLevel login(const AuthenticationRequest& auth_request) = 0;

void
XClient::login(AuthenticationRequest authreq)
  CODE:
    TRY {
      THIS->login(authreq);
    } CATCH;


# virtual void set_keyspace(const std::string& keyspace) = 0;

void
XClient::set_keyspace(string keyspace)
  CODE:
    TRY {
      THIS->set_keyspace(keyspace);
    } CATCH;


# virtual void get(ColumnOrSuperColumn& _return, const std::string& key, const ColumnPath& column_path, const ConsistencyLevel consistency_level) = 0;

ColumnOrSuperColumn
XClient::_get(string key, ColumnPath column_path, ConsistencyLevel consistency_level)
  CODE:
    TRY {
      THIS->get(RETVAL, key, column_path, consistency_level);
    } CATCH;
  OUTPUT:
    RETVAL

# virtual void get_slice(std::vector<ColumnOrSuperColumn> & _return, const std::string& key, const ColumnParent& column_parent, const SlicePredicate& predicate, const ConsistencyLevel consistency_level) = 0;

vector<ColumnOrSuperColumn>
XClient::_get_slice(string key, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level)
  CODE:
    TRY {
      THIS->get_slice(RETVAL, key, column_parent, predicate, consistency_level);
    } CATCH;
  OUTPUT:
    RETVAL

# virtual void insert(const std::string& key, const ColumnParent& column_parent, const Column& column, const ConsistencyLevel consistency_level) = 0;

void
XClient::_insert(string key, ColumnParent column_parent, Column column, ConsistencyLevel consistency_level)
  CODE:
    TRY {
      THIS->insert(key, column_parent, column, consistency_level);
    } CATCH;

# virtual void remove(const std::string& key, const ColumnPath& column_path, const int64_t timestamp, const ConsistencyLevel consistency_level) = 0;

void
XClient::_remove(string key, ColumnPath column_path, int64_t timestamp, ConsistencyLevel consistency_level)
  CODE:
    TRY {
      THIS->remove(key, column_path, timestamp, consistency_level);
    } CATCH;

# virtual void batch_mutate(const std::map<std::string, std::map<std::string, std::vector<Mutation> > > & mutation_map, const ConsistencyLevel consistency_level) = 0;

=for never


  virtual int32_t get_count(const std::string& key, const ColumnParent& column_parent, const SlicePredicate& predicate, const ConsistencyLevel consistency_level) = 0;
  virtual void multiget_slice(std::map<std::string, std::vector<ColumnOrSuperColumn> > & _return, const std::vector<std::string> & keys, const ColumnParent& column_parent, const SlicePredicate& predicate, const ConsistencyLevel consistency_level) = 0;
  virtual void multiget_count(std::map<std::string, int32_t> & _return, const std::string& keyspace, const std::vector<std::string> & keys, const ColumnParent& column_parent, const SlicePredicate& predicate, const ConsistencyLevel consistency_level) = 0;
  virtual void get_range_slices(std::vector<KeySlice> & _return, const ColumnParent& column_parent, const SlicePredicate& predicate, const KeyRange& range, const ConsistencyLevel consistency_level) = 0;
  virtual void get_indexed_slices(std::vector<KeySlice> & _return, const ColumnParent& column_parent, const IndexClause& index_clause, const SlicePredicate& column_predicate, const ConsistencyLevel consistency_level) = 0;
  virtual void insert(const std::string& key, const ColumnParent& column_parent, const Column& column, const ConsistencyLevel consistency_level) = 0;
  virtual void remove(const std::string& key, const ColumnPath& column_path, const Clock& clock, const ConsistencyLevel consistency_level) = 0;
  virtual void batch_mutate(const std::map<std::string, std::map<std::string, std::vector<Mutation> > > & mutation_map, const ConsistencyLevel consistency_level) = 0;
  virtual void truncate(const std::string& cfname) = 0;
  virtual void check_schema_agreement(std::map<std::string, std::vector<std::string> > & _return) = 0;
  virtual void describe_keyspaces(std::vector<KsDef> & _return) = 0;
  virtual void describe_cluster_name(std::string& _return) = 0;
  virtual void describe_version(std::string& _return) = 0;
  virtual void describe_ring(std::vector<TokenRange> & _return, const std::string& keyspace) = 0;
  virtual void describe_partitioner(std::string& _return) = 0;
  virtual void describe_keyspace(KsDef& _return, const std::string& keyspace) = 0;
  virtual void describe_splits(std::vector<std::string> & _return, const std::string& keyspace, const std::string& cfName, const std::string& start_token, const std::string& end_token, const int32_t keys_per_split) = 0;
  virtual void system_add_column_family(std::string& _return, const CfDef& cf_def) = 0;
  virtual void system_drop_column_family(std::string& _return, const std::string& column_family) = 0;
  virtual void system_rename_column_family(std::string& _return, const std::string& old_name, const std::string& new_name) = 0;
  virtual void system_add_keyspace(std::string& _return, const KsDef& ks_def) = 0;
  virtual void system_drop_keyspace(std::string& _return, const std::string& keyspace) = 0;
  virtual void system_rename_keyspace(std::string& _return, const std::string& old_name, const std::string& new_name) = 0;

=cut
