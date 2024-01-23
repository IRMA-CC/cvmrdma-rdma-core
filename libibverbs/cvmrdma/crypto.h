#pragma once

#include <assert.h>

#include <sodium.h>

enum sodium_crypto_aead {
    SODIUM_AEAD_NONE,
    SODIUM_AEAD_XCHACHAPOLY,
    SODIUM_AEAD_AEGIS128L,
    SODIUM_AEAD_AES256GCM,
    __SODIUM_AEAD_MAX = SODIUM_AEAD_AES256GCM
};
#define __SODIUM_AEAD_NR (__SODIUM_AEAD_MAX + 1)


/* Keys: XChachaPoly=32B AES256-GCM=32B AEGIS128L=16B */
#define cvmrdma_aead_KEY_WORDS 4
#define cvmrdma_aead_KEYBYTES (cvmrdma_aead_KEY_WORDS * sizeof(uint64_t))

/* Nonces: XChachaPoly=24B AES256-GCM=12B AEGIS128L=16B */
#define cvmrdma_aead_NPUB_WORDS 3
#define cvmrdma_aead_NPUBBYTES (cvmrdma_aead_NPUB_WORDS * sizeof(uint64_t))

/* MAC: XChachaPoly=16B AES256-GCM=16B AEGIS128L=32B */
#define cvmrdma_aead_A_WORDS 4
#define cvmrdma_aead_ABYTES (cvmrdma_aead_A_WORDS * sizeof(uint64_t))

static inline unsigned long long cvmrdma_ABYTES(enum sodium_crypto_aead aead)
{
    switch(aead)
    {
        case SODIUM_AEAD_NONE:
            return 0;
        case SODIUM_AEAD_XCHACHAPOLY:
            return crypto_aead_xchacha20poly1305_ietf_ABYTES;
        case SODIUM_AEAD_AEGIS128L:
            return crypto_aead_aegis128l_ABYTES;
        case SODIUM_AEAD_AES256GCM:
            return crypto_aead_aegis128l_ABYTES;
    }
}


static inline int cvmrdma_encrypt(unsigned char *c,
                    unsigned long long *clen_p,
                    const unsigned char *m,
                    unsigned long long mlen,
                    const unsigned char *ad,
                    unsigned long long adlen,
                    const unsigned char *nsec,
                    const unsigned char *npub,
                    const unsigned char *k,
                    enum sodium_crypto_aead aead)
{
    switch(aead) {
        case SODIUM_AEAD_NONE: {
            memcpy(c, m, mlen);
            *clen_p = mlen;
            return 0;
        }
        case SODIUM_AEAD_XCHACHAPOLY:
            return crypto_aead_xchacha20poly1305_ietf_encrypt(c, clen_p, m, mlen, ad, adlen, nsec, npub, k);
        case SODIUM_AEAD_AEGIS128L:
            return crypto_aead_aes256gcm_encrypt(c, clen_p, m, mlen, ad, adlen, nsec, npub, k);
        case SODIUM_AEAD_AES256GCM:
            return crypto_aead_aes256gcm_encrypt(c, clen_p, m, mlen, ad, adlen, nsec, npub, k);
    }
}


static inline int cvmrdma_decrypt(unsigned char *m,
                    unsigned long long *mlen_p,
                    unsigned char *nsec,
                    const unsigned char *c,
                    unsigned long long clen,
                    const unsigned char *ad,
                    unsigned long long adlen,
                    const unsigned char *npub,
                    const unsigned char *k,
                    enum sodium_crypto_aead aead)
    {
    switch(aead) {
        case SODIUM_AEAD_NONE: {
            memcpy(m, c, clen);
            *mlen_p = clen;
            return 0;
        }
        case SODIUM_AEAD_XCHACHAPOLY:
            return crypto_aead_xchacha20poly1305_ietf_decrypt(m, mlen_p, nsec, c, clen, ad, adlen, npub, k);
        case SODIUM_AEAD_AEGIS128L:
            return crypto_aead_aes256gcm_decrypt(m, mlen_p, nsec, c, clen, ad, adlen, npub, k);
        case SODIUM_AEAD_AES256GCM:
            return crypto_aead_aes256gcm_decrypt(m, mlen_p, nsec, c, clen, ad, adlen, npub, k);
    }
}