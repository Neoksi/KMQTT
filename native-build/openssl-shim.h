/*
 * Shim для cinterop поверх сгенерированных заголовков OpenSSL 3.5.
 *
 * OpenSSL 3.5 на этапе `./Configure`/`make build_generated` превращает типизированные
 * функции стека (sk_<T>_num/value/pop_free/...) из safestack.h в function-like МАКРОСЫ
 * (см. сгенерированный x509.h: `SKM_DEFINE_STACK_OF_INTERNAL` + `#define sk_X509_INFO_num(sk) ...`).
 * cinterop макросы не индексирует, поэтому форк (legacy TLSClientEngine, OpenSslSession)
 * не видит эти символы. Здесь нужные форку четыре функции возвращаются как `static inline`
 * с теми же именами и сигнатурами, что в 1.1.1w: код форка не меняется.
 *
 * Подключается ПОСЛЕДНИМ в openssl-3.5.def. Ровно те функции, которые использует форк:
 * sk_X509_INFO_num/value/pop_free (TLSClientEngine каждой цели) и sk_X509_OBJECT_num
 * (OpenSslSession). Если понадобятся ещё — дополнять здесь, не правя код форка.
 */
#include <openssl/x509.h>

#undef sk_X509_INFO_num
#undef sk_X509_INFO_value
#undef sk_X509_INFO_pop_free
#undef sk_X509_OBJECT_num

static inline int sk_X509_INFO_num(const STACK_OF(X509_INFO) *sk)
{
    return OPENSSL_sk_num((const OPENSSL_STACK *)sk);
}

static inline X509_INFO *sk_X509_INFO_value(const STACK_OF(X509_INFO) *sk, int idx)
{
    return (X509_INFO *)OPENSSL_sk_value((const OPENSSL_STACK *)sk, idx);
}

static inline void sk_X509_INFO_pop_free(STACK_OF(X509_INFO) *sk, sk_X509_INFO_freefunc freefunc)
{
    OPENSSL_sk_pop_free((OPENSSL_STACK *)sk, (OPENSSL_sk_freefunc)freefunc);
}

static inline int sk_X509_OBJECT_num(const STACK_OF(X509_OBJECT) *sk)
{
    return OPENSSL_sk_num((const OPENSSL_STACK *)sk);
}
