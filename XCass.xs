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
#include <transport/TSocket.h>
#include <transport/TBufferTransports.h>
#include <protocol/TBinaryProtocol.h>
#include <memory>

using namespace std;
using namespace org::apache::cassandra;
using namespace apache::thrift;  // assuming no conflict

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

static void assign_to_map(pTHX_ map<string,string> &m, SV *sv) {
    if (SvTYPE(sv) != SVt_PVHV)
        croak("expected hash to initialize map");

    HV *hv = (HV *)sv;
    hv_iterinit(hv);

    SV *valsv;
    char *kp;
    I32 klen;
    while ((valsv = hv_iternextsv(hv, &kp, &klen)) && klen >= 0) {
        STRLEN vlen;
        const char *vp = SvPV(valsv, vlen);
        m[string(kp, klen)] = string(vp, vlen);
    }
}

static AuthenticationRequest *new_authreq(pTHX_ SV *sv) {
    auto_ptr<AuthenticationRequest> ar;
    assign_to_map(aTHX_ ar->credentials, sv);
    return ar.release();
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


=for never

  virtual AccessLevel login(const AuthenticationRequest& auth_request) = 0;
  virtual void set_keyspace(const std::string& keyspace) = 0;
  virtual void get(ColumnOrSuperColumn& _return, const std::string& key, const ColumnPath& column_path, const ConsistencyLevel consistency_level) = 0;
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
