// @ts-check
import { themes as prismThemes } from 'prism-react-renderer';

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'EventFlux',
  tagline: 'The Rust-native CEP Engine',
  favicon: 'img/logo.png',

  url: 'https://eventflux.io',
  baseUrl: '/',

  organizationName: 'eventflux-io',
  projectName: 'eventflux',

  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  // Enable Mermaid diagrams in Markdown
  markdown: {
    mermaid: true,
  },
  themes: ['@docusaurus/theme-mermaid'],

  // Google Fonts for typography
  headTags: [
    {
      tagName: 'link',
      attributes: {
        rel: 'preconnect',
        href: 'https://fonts.googleapis.com',
      },
    },
    {
      tagName: 'link',
      attributes: {
        rel: 'preconnect',
        href: 'https://fonts.gstatic.com',
        crossorigin: 'anonymous',
      },
    },
    {
      tagName: 'link',
      attributes: {
        rel: 'stylesheet',
        href: 'https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap',
      },
    },
  ],

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: './sidebars.js',
          editUrl: 'https://github.com/eventflux-io/eventflux/tree/main/website/',
        },
        blog: {
          showReadingTime: true,
          feedOptions: {
            type: ['rss', 'atom'],
            xslt: true,
          },
          editUrl: 'https://github.com/eventflux-io/eventflux/tree/main/website/',
        },
        theme: {
          customCss: './src/css/custom.css',
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      announcementBar: {
        id: 'star_us',
        content:
          'If you find EventFlux useful, give us a <a target="_blank" rel="noopener noreferrer" href="https://github.com/eventflux-io/eventflux">star on GitHub</a>!',
        backgroundColor: '#C97C5D',
        textColor: '#FFFFFF',
        isCloseable: true,
      },
      image: 'img/eventflux-social-card.png',
      colorMode: {
        defaultMode: 'dark',
        disableSwitch: false,
        respectPrefersColorScheme: true,
      },
      navbar: {
        title: 'EventFlux',
        logo: {
          alt: 'EventFlux Logo',
          src: 'img/logo.png',
          srcDark: 'img/logo.png',
          width: 32,
          height: 32,
        },
        items: [
          {
            type: 'docSidebar',
            sidebarId: 'tutorialSidebar',
            position: 'left',
            label: 'Docs',
          },
          {
            to: '/docs/demo/crypto-trading',
            label: 'Demo',
            position: 'left',
          },
          {
            to: '/docs/rust-api/getting-started',
            label: 'API',
            position: 'left',
          },
          { to: '/blog', label: 'Blog', position: 'left' },
          {
            href: 'https://github.com/eventflux-io/eventflux',
            label: 'GitHub',
            position: 'right',
          },
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Documentation',
            items: [
              {
                label: 'Introduction',
                to: '/docs/intro',
              },
              {
                label: 'Quick Start',
                to: '/docs/getting-started/quick-start',
              },
              {
                label: 'SQL Reference',
                to: '/docs/sql-reference/queries',
              },
              {
                label: 'Windows',
                to: '/docs/sql-reference/windows',
              },
            ],
          },
          {
            title: 'Community',
            items: [
              {
                label: 'GitHub Discussions',
                href: 'https://github.com/eventflux-io/eventflux/discussions',
              },
              {
                label: 'Discord',
                href: 'https://discord.gg/eventflux',
              },
              {
                label: 'Twitter',
                href: 'https://twitter.com/eventflux_io',
              },
            ],
          },
          {
            title: 'More',
            items: [
              {
                label: 'Blog',
                to: '/blog',
              },
              {
                label: 'GitHub',
                href: 'https://github.com/eventflux-io/eventflux',
              },
              {
                label: 'Architecture',
                to: '/docs/architecture/overview',
              },
              {
                label: 'Rust API',
                to: '/docs/rust-api/getting-started',
              },
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} EventFlux Project.`,
      },
      prism: {
        theme: prismThemes.github,
        darkTheme: prismThemes.nightOwl,
        additionalLanguages: ['rust', 'sql', 'toml', 'bash'],
      },
      algolia: {
        appId: 'MMPU9F9NTH',
        apiKey: '0888f17cb65bf83ed95f3bdbda85e4af',
        indexName: 'EventFlux Documentation',
        contextualSearch: true,
      },
    }),
};

export default config;
