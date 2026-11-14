# DIUBAH untuk iconics-fep-iec61850 (lihat THIRD_PARTY_NOTICES.md): hanya contoh yang dipakai
# image dev (simulator IED + mms_utility). Contoh lain tidak disertakan di native/libiec61850.
add_subdirectory(server_example_basic_io)

if (NOT WIN32)
    add_subdirectory(mms_utility)
endif(NOT WIN32)
