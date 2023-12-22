#pragma once

#include <cassert>
#include <cinttypes>
#include <array>
#include <sodium.h>

static_assert((crypto_aead_aegis256_NPUBBYTES / sizeof(uint64_t)) == 4, "");
static_assert((crypto_aead_aegis256_ABYTES / sizeof(uint64_t)) == 4, "");

constexpr uint64_t aegis_256_KEYWORDS = crypto_aead_aegis256_KEYBYTES / sizeof(uint64_t);
using aegis_key_t = std::array<uint64_t, aegis_256_KEYWORDS>;