$pdf_mode = 5;   # xelatex -> xdv -> xdvipdfmx

$halt_on_error = 1;

$interaction = 'nonstopmode';

$synctex = 1;

$xelatex = 'xelatex '
         . '-file-line-error '
         . '-halt-on-error '
         . '-interaction=nonstopmode '
         . '-no-pdf '
         . '-synctex=1 '
         . '%O %S';

$xdvipdfmx = 'xdvipdfmx -q -E -o %D %O %S';

$bibtex_use = 1.5;

$bibtex = 'bibtex %O %B';

$clean_ext = join ' ',
    qw(
        thm
        loa
        nav
        snm
        vrb
        synctex.gz
    );

$makeindex = 'makeindex %O -o %D %S';

add_cus_dep('glo', 'gls', 0, 'glo2gls');
sub glo2gls {
    system("makeindex -s gglo.ist -o \"$_[0].gls\" \"$_[0].glo\"");
}
push @generated_exts, 'glo', 'gls';

add_cus_dep('nlo', 'nls', 0, 'nlo2nls');
sub nlo2nls {
    system("makeindex -s nomencl.ist -o \"$_[0].nls\" \"$_[0].nlo\"");
}
push @generated_exts, 'nlo', 'nls';

$enable_write18 = 0;

$recorder = 1;