#pragma once

#include <assert.h>

#include <sodium.h>

#define crypto_aead_xchacha20poly1305_ietf_KEY_WORDS 4
#define crypto_aead_xchacha20poly1305_ietf_NPUB_WORDS 3
#define crypto_aead_xchacha20poly1305_ietf_A_WORDS 2

static_assert(crypto_aead_xchacha20poly1305_ietf_KEYBYTES == (sizeof(uint64_t) * crypto_aead_xchacha20poly1305_ietf_KEY_WORDS), "");
static_assert(crypto_aead_xchacha20poly1305_ietf_NPUBBYTES == (sizeof(uint64_t) * crypto_aead_xchacha20poly1305_ietf_NPUB_WORDS), "");
static_assert(crypto_aead_xchacha20poly1305_ietf_ABYTES == (sizeof(uint64_t) * crypto_aead_xchacha20poly1305_ietf_A_WORDS), "");