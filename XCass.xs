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


static SV *pl_av_fetch_safe(pTHX_ AV *av, I32 index) {
    SV **svp = av_fetch(av, index, 0);
    return svp ? *svp : &PL_sv_undef;
}
#define av_fetch_safe(av,index)  pl_av_fetch_safe(aTHX_ av,index)


static SV *pl_hv_fetch_safe(pTHX_ HV *hv, const char *key, I32 klen) {
    SV **svp = hv_fetch(hv, key, klen, 0);
    return svp ? *svp : &PL_sv_undef;
}
#define hv_fetch_safe(hv,key,klen)  pl_hv_fetch_safe(aTHX_ hv,key,klen)
#define hv_fetchs_safe(hv,key)      pl_hv_fetch_safe(aTHX_ hv,key,sizeof(key)-1)


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
#define output_thrift(t)     pl_output_thrift(aTHX_ t)

// AuthenticationRequest

static void pl_assign_thrift(pTHX_ AuthenticationRequest &ar, SV *src_sv) {
    assign_map(aTHX_ ar.credentials, src_sv);
}

// ColumnPath

static void pl_assign_thrift(pTHX_ ColumnPath &cp, SV *src_sv) {
    bool has_family = false;
    switch (SvTYPE(src_sv)) {
      case SVt_PVAV: {
        AV *av = (AV*)src_sv;
        assign_string_maybe(cp.column_family, has_family,               av_fetch_safe(av, 0));
        assign_string_maybe(cp.super_column,  cp.__isset.super_column,  av_fetch_safe(av, 1));
        assign_string_maybe(cp.column,        cp.__isset.column,        av_fetch_safe(av, 2));
        break;
      }

      case SVt_PVHV: {
        HV *hv = (HV*)src_sv;
        assign_string_maybe(cp.column_family, has_family,              hv_fetchs_safe(hv, "family"));
        assign_string_maybe(cp.super_column,  cp.__isset.super_column, hv_fetchs_safe(hv, "super_column"));
        assign_string_maybe(cp.column,        cp.__isset.column,       hv_fetchs_safe(hv, "column"));
        break;
      }

      default:
        assign_string_maybe(cp.column_family, has_family,              src_sv);
        break;
    }
    if (!has_family)
        throw "Missing column family in column path";
}

// Clock

static void pl_assign_thrift(pTHX_ Clock &clock, SV *sv) {
    SvGETMAGIC(sv);
#if IVSIZE > 4
    if (SvIOK(sv))
        clock.timestamp = SvIOK_UV(sv) ? (int64_t)SvUVX(sv) : (int64_t)SvIVX(sv);
    else
#endif
    if (SvNOK(sv))
        clock.timestamp = SvNVX(sv);
    else {
        char *e;
        clock.timestamp = strtoll(SvPV_nolen(sv), &e, 0);
        if (*e)
            warn("invalid numeric characters in timestamp: %_", sv);
    }
}

static SV *pl_output_thrift(pTHX_ const Clock &clock) {
#if IVSIZE > 4
    return newSViv(clock.timestamp);
#else
    return newSVnv((NV)clock.timestamp);
#endif
}

// Column

static SV *pl_output_thrift(pTHX_ const Column &col) {
    AV *av = newAV();
    av_push(av, newSVstring(col.value));
    av_push(av, output_thrift(col.clock));
    if (col.__isset.ttl)
        av_push(av, newSViv(col.ttl));
}

// SuperColumn

static SV *pl_output_thrift(pTHX_ const SuperColumn &sc) {
    AV *columns = newAV();
    for (size_t i = 0; i < sc.columns.size(); ++i)
        av_push(columns, output_thrift(sc.columns[i]));

    HV *hv = newHV();
    hv_stores(hv, "name",    newSVstring(sc.name));
    hv_stores(hv, "columns", (SV*)columns);
    return (SV*)hv;
}

// ColumnOrSuperColumn

static SV *pl_output_thrift(pTHX_ const ColumnOrSuperColumn &cos) {
    if (cos.__isset.super_column)
        return output_thrift(cos.super_column);
    else if (cos.__isset.column)
        return output_thrift(cos.column);
    else
        return &PL_sv_undef;
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

AccessLevel
XClient::login(AuthenticationRequest authreq)
  CODE:
    TRY {
      RETVAL = THIS->login(authreq);
    } CATCH;
  OUTPUT:
    RETVAL


# virtual void set_keyspace(const std::string& keyspace) = 0;

void
XClient::set_keyspace(string keyspace)
  CODE:
    TRY {
      THIS->set_keyspace(keyspace);
    } CATCH;


# virtual void get(ColumnOrSuperColumn& _return, const std::string& key, const ColumnPath& column_path, const ConsistencyLevel consistency_level) = 0;

ColumnOrSuperColumn
XClient::_get(string key, ColumnPath colpath, ConsistencyLevel conlev)
  CODE:
    TRY {
      THIS->get(RETVAL, key, colpath, conlev);
    } CATCH;
  OUTPUT:
    RETVAL


=for never


  virtual void get_slice(std::vector<ColumnOrSuperColumn> & _return, const std::string& key, const ColumnParent& column_parent, const SlicePredicate& predicate, const ConsistencyLevel consistency_level) = 0;
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
