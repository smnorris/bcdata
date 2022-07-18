rm -rf cli.md
printf '### CLI' > cli.md
printf '\n' >> cli.md

printf 'Commands available via the bcdata command line interface are documented with the --help option\n' >> cli.md
printf '\n' >> cli.md
printf '```\n\n$ bcdata --help\n\n' >> cli.md
bcdata --help >> cli.md
printf '```\n' >> cli.md

printf '\n' >> cli.md
printf '#### bc2pg\n\n' >> cli.md
printf '```\n$ bcdata bc2pg --help\n\n' >> cli.md
bcdata bc2pg --help >> cli.md
printf '```\n' >> cli.md

printf '\n' >> cli.md
printf '#### cat\n\n' >> cli.md
printf '```\n$ bcdata cat --help\n\n' >> cli.md
bcdata cat --help >> cli.md
printf '```\n' >> cli.md

printf '\n' >> cli.md
printf '#### dem \n\n' >> cli.md
printf '```\n$ bcdata dem --help\n\n' >> cli.md
bcdata dem --help >> cli.md
printf '```\n' >> cli.md

printf '\n' >> cli.md
printf '#### dump \n\n' >> cli.md
printf '```\n$ bcdata dump --help\n\n' >> cli.md
bcdata dump --help >> cli.md
printf '```\n' >> cli.md

printf '\n' >> cli.md
printf '#### info\n\n' >> cli.md
printf '```\n$ bcdata info --help\n\n' >> cli.md
bcdata info --help >> cli.md
printf '```\n' >> cli.md

printf '\n' >> cli.md
printf '#### list\n\n' >> cli.md
printf '```\n$ bcdata list --help\n\n' >> cli.md
bcdata list --help >> cli.md
printf '```\n' >> cli.md