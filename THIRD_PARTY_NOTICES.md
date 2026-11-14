# Third-party notices

## libIEC61850

| | |
|---|---|
| Versi | **v1.6.2.1** (tag git `v1.6.2.1`) |
| Commit | `6e7a603c771d6124cd3cafea7ec38b7e0f2e7aaf` |
| Sumber | https://github.com/mz-automation/libiec61850 |
| Pembuat | MZ Automation GmbH |
| Lisensi | **GPLv3** (lihat [native/libiec61850/COPYING](native/libiec61850/COPYING)) atau lisensi komersial dari MZ Automation |
| Lokasi di repo | [native/libiec61850/](native/libiec61850/) |
| Dipakai oleh | `Dockerfile` (produksi) dan `docker/Dockerfile.dev` (stage `libiec61850`): build dari folder ini, tanpa unduh dari internet. Di-link statis ke binary `fep-iec61850` dan `tools/iec61850-cli`. |

### Perubahan dari source asli

Source disalin dari tag di atas. Perubahannya:

- Folder `.git` tidak disertakan.
- `examples/`: hanya `server_example_basic_io` (simulator IED + `simpleIO_direct_control.cid`) dan
  `mms_utility` yang disertakan. Keduanya dipakai `docker/Dockerfile.dev`. Contoh lain dihapus.
- `examples/CMakeLists.txt` **diubah**: hanya memanggil `add_subdirectory` untuk dua contoh di atas.
- Test & fuzzing yang tidak dipakai dihapus: `fuzz/`, `dotnet/tests/`, `pyiec61850/test_pyiec61850.py`.

File lain (termasuk `COPYING`, `README.md`, `CHANGELOG`) tidak diubah.

### Lisensi

libIEC61850 berlisensi ganda: GPLv3 atau komersial. FEP me-link libIEC61850 **secara statis**.
Kalau binary FEP didistribusikan di bawah GPLv3, source lengkap (termasuk folder ini) harus ikut
tersedia. Kalau tidak, dibutuhkan lisensi komersial MZ Automation. Status lisensi masih menunggu
konfirmasi tim (docs/CATATAN_TERBUKA.md no. 11).

### Ganti versi

1. Unduh source tag baru dari https://github.com/mz-automation/libiec61850.
2. Ganti isi `native/libiec61850/` dengan source itu, lalu ulangi perubahan di atas (hapus `.git`, contoh
   yang tidak dipakai, dan test; sesuaikan `examples/CMakeLists.txt`).
3. Perbarui versi & commit di file ini, lalu build ulang image dev dan produksi.
