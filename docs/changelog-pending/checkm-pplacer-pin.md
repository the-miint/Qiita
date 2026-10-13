### Fixed

- **The long-read-assembly `checkm` image builds again (#701).** Its pin list named pplacer
  `1.1.alpha22`, which no bioconda build of checkm-genome 1.2.5 accepts (each requires
  `1.1.alpha19` or `1.1.alpha20` exactly), so the solve failed and the deploy aborted at
  the image build. The pin is now `1.1.alpha20`, the version both deployed `checkm` images
  (`-1.0.0` and `-1.0.1`) carry.
