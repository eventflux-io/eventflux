import React from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import { motion } from 'framer-motion';
import ScenarioBlock from '@site/src/components/ScenarioBlock';
import styles from './index.module.css';

// Hero Section Component
function HeroSection() {
  return (
    <header className={clsx('hero', styles.heroBanner)}>
      <div className={styles.heroGradient} />
      <div className="container">
        <motion.div
          initial={{ opacity: 0, y: 30 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.8, ease: [0.25, 0.1, 0.25, 1] }}
        >
          <h1 className={styles.heroTitle}>
            Stream Processing,
            <br />
            <span className={styles.heroHighlight}>Reimagined in Rust</span>
          </h1>
        </motion.div>

        <motion.p
          className={styles.heroSubtitle}
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.8, delay: 0.2, ease: [0.25, 0.1, 0.25, 1] }}
        >
          EventFlux is a high-performance Complex Event Processing (CEP) engine
          built from the ground up in Rust. Process millions of events per second
          with SQL-like queries, pattern matching, and real-time analytics.
        </motion.p>

        <motion.div
          className={styles.heroButtons}
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.8, delay: 0.4, ease: [0.25, 0.1, 0.25, 1] }}
        >
          <Link
            className="button button--primary button--lg"
            to="/docs/intro"
          >
            Get Started
          </Link>
          <Link
            className="button button--secondary button--lg"
            to="https://github.com/eventflux-io/eventflux"
          >
            View on GitHub
          </Link>
        </motion.div>

        <motion.div
          className={styles.heroStats}
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ duration: 0.8, delay: 0.6 }}
        >
          <div className={styles.stat}>
            <span className={styles.statNumber}>1M+</span>
            <span className={styles.statLabel}>Events/sec</span>
          </div>
          <div className={styles.stat}>
            <span className={styles.statNumber}>100%</span>
            <span className={styles.statLabel}>Memory Safe</span>
          </div>
          <div className={styles.stat}>
            <span className={styles.statNumber}>SQL</span>
            <span className={styles.statLabel}>First Design</span>
          </div>
        </motion.div>
      </div>
    </header>
  );
}

// Features Section with scroll-triggered scenarios
function FlowSection() {
  const scenarios = [
    {
      title: 'High-Frequency Trading',
      icon: '📈',
      scenario:
        'Detect arbitrage opportunities by correlating price feeds from multiple exchanges in real-time. Trigger alerts when price spreads exceed thresholds within time windows.',
      code: `-- Detect arbitrage opportunities across exchanges
SELECT a.symbol, a.price AS price_a, b.price AS price_b,
       ABS(a.price - b.price) AS spread
FROM ExchangeA AS a
WINDOW TUMBLING(1 sec)
JOIN ExchangeB AS b
  ON a.symbol = b.symbol
WHERE ABS(a.price - b.price) / a.price > 0.001
INSERT INTO ArbitrageAlerts;`,
      language: 'sql',
      reversed: false,
    },
    {
      title: 'IoT Anomaly Detection',
      icon: '🌡️',
      scenario:
        'Monitor thousands of IoT sensors simultaneously. Detect anomalies when temperature readings exceed thresholds, triggering maintenance alerts before equipment fails.',
      code: `-- Detect temperature anomalies in IoT sensors
SELECT sensor_id,
       AVG(temperature) AS avg_temp,
       MAX(temperature) AS max_temp,
       MIN(temperature) AS min_temp,
       COUNT(*) AS reading_count
FROM SensorReadings
WINDOW TUMBLING(5 min)
GROUP BY sensor_id
HAVING MAX(temperature) > 100
    OR MIN(temperature) < 0
INSERT INTO AnomalyAlerts;`,
      language: 'sql',
      reversed: true,
    },
    {
      title: 'Fraud Detection Patterns',
      icon: '🔒',
      scenario:
        'Identify fraudulent transaction patterns by detecting sequences of events that match known fraud signatures. Flag accounts with suspicious rapid transaction bursts.',
      code: `-- Detect suspicious transaction patterns
SELECT account_id,
       COUNT(*) AS tx_count,
       SUM(amount) AS total_amount,
       AVG(amount) AS avg_amount
FROM Transactions
WINDOW SESSION(10 min, account_id)
GROUP BY account_id
HAVING COUNT(*) > 5
   AND SUM(amount) > 10000
INSERT INTO FraudAlerts;`,
      language: 'sql',
      reversed: false,
    },
    {
      title: 'Dynamic Risk Classification',
      icon: '⚖️',
      scenario:
        'Classify incoming events dynamically using SQL CASE expressions. Categorize transactions by risk level based on multiple conditions and route them to appropriate handlers.',
      code: `-- Classify transactions by risk level
SELECT tx_id, amount, country,
       CASE
           WHEN amount > 10000 AND country NOT IN ('US', 'UK')
               THEN 'HIGH_RISK'
           WHEN amount > 5000 THEN 'MEDIUM_RISK'
           WHEN amount > 1000 THEN 'LOW_RISK'
           ELSE 'MINIMAL_RISK'
       END AS risk_level,
       CASE type
           WHEN 'WIRE' THEN 'Requires Review'
           WHEN 'ACH' THEN 'Auto-process'
           ELSE 'Manual Check'
       END AS action
FROM Transactions
INSERT INTO RiskClassified;`,
      language: 'sql',
      reversed: true,
    },
    {
      title: 'RabbitMQ Event Pipeline',
      icon: '🐰',
      scenario:
        'Build end-to-end streaming pipelines with RabbitMQ. Consume JSON events from queues, process with SQL queries, and publish results to exchanges. Perfect for microservices architectures.',
      code: `-- RabbitMQ Source: Consume from queue
CREATE STREAM TradeInput (
    symbol STRING, price DOUBLE, volume INT
) WITH (
    type = 'source', extension = 'rabbitmq',
    format = 'json',
    "rabbitmq.host" = 'localhost',
    "rabbitmq.queue" = 'trades'
);

-- RabbitMQ Sink: Publish to exchange
CREATE STREAM TradeOutput (
    symbol STRING, price DOUBLE, category STRING
) WITH (
    type = 'sink', extension = 'rabbitmq',
    format = 'json',
    "rabbitmq.host" = 'localhost',
    "rabbitmq.exchange" = 'processed-trades'
);

-- Process: Filter and classify
INSERT INTO TradeOutput
SELECT symbol, price,
       CASE WHEN price > 100 THEN 'high' ELSE 'low' END
FROM TradeInput WHERE volume > 1000;`,
      language: 'sql',
      reversed: false,
    },
    {
      title: 'Real-Time State Management',
      icon: '🗃️',
      scenario:
        'Maintain synchronized state tables from streaming data. Use UPSERT to insert new records or update existing ones, UPDATE to modify specific fields, and DELETE to remove stale entries—all triggered by stream events.',
      code: `-- Product catalog table
CREATE TABLE ProductCatalog (
    sku STRING, name STRING, price DOUBLE, stock INT
) WITH (extension = 'inMemory');

-- Incoming inventory updates
CREATE STREAM InventoryStream (
    sku STRING, name STRING, price DOUBLE, stock INT
);

-- UPSERT: Insert new products or update existing
UPSERT INTO ProductCatalog
SELECT sku, name, price, stock
FROM InventoryStream
ON ProductCatalog.sku = InventoryStream.sku;

-- UPDATE: Adjust prices from a separate stream
CREATE STREAM PriceUpdateStream (sku STRING, newPrice DOUBLE);
UPDATE ProductCatalog SET price = PriceUpdateStream.newPrice
FROM PriceUpdateStream
WHERE ProductCatalog.sku = PriceUpdateStream.sku;

-- DELETE: Remove discontinued products
CREATE STREAM DiscontinuedStream (sku STRING);
DELETE FROM ProductCatalog
USING DiscontinuedStream
WHERE ProductCatalog.sku = DiscontinuedStream.sku;`,
      language: 'sql',
      reversed: true,
    },
  ];

  return (
    <section className={styles.flowSection}>
      <div className="container">
        <motion.div
          className={styles.sectionHeader}
          initial={{ opacity: 0, y: 30 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true, margin: '-100px' }}
          transition={{ duration: 0.6 }}
        >
          <h2 className={styles.sectionTitle}>
            Real-World Event Processing
          </h2>
          <p className={styles.sectionSubtitle}>
            See how EventFlux handles complex streaming scenarios with elegant SQL queries
          </p>
        </motion.div>

        <div className={styles.scenariosContainer}>
          {scenarios.map((scenario, index) => (
            <ScenarioBlock
              key={index}
              title={scenario.title}
              icon={scenario.icon}
              scenario={scenario.scenario}
              code={scenario.code}
              language={scenario.language}
              reversed={scenario.reversed}
            />
          ))}
        </div>
      </div>
    </section>
  );
}

// Features Grid Section
function FeaturesSection() {
  const features = [
    {
      title: 'Blazing Fast',
      icon: '⚡',
      description:
        'Lock-free data structures and zero-allocation hot paths deliver over 1 million events per second.',
    },
    {
      title: 'SQL-First',
      icon: '🔤',
      description:
        'Write queries in familiar SQL with streaming extensions. No new language to learn.',
    },
    {
      title: 'Memory Safe',
      icon: '🛡️',
      description:
        "Built in Rust with zero unsafe code in the hot path. No garbage collection pauses.",
    },
    {
      title: 'Pattern Matching',
      icon: '🔍',
      description:
        'Detect complex event patterns with temporal constraints and correlation rules.',
    },
    {
      title: 'Stateful Processing',
      icon: '💾',
      description:
        'Enterprise-grade state management with incremental checkpointing and recovery.',
    },
    {
      title: 'Distributed Ready',
      icon: '🌐',
      description:
        'Scale horizontally with built-in support for clustering and partition-aware processing.',
    },
  ];

  return (
    <section className={styles.featuresSection}>
      <div className="container">
        <motion.div
          className={styles.sectionHeader}
          initial={{ opacity: 0, y: 30 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true, margin: '-100px' }}
          transition={{ duration: 0.6 }}
        >
          <h2 className={styles.sectionTitle}>Why EventFlux?</h2>
          <p className={styles.sectionSubtitle}>
            Built for the demands of modern real-time data processing
          </p>
        </motion.div>

        <div className={styles.featuresGrid}>
          {features.map((feature, index) => (
            <motion.div
              key={index}
              className={styles.featureCard}
              initial={{ opacity: 0, y: 30 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true, margin: '-50px' }}
              transition={{ duration: 0.5, delay: index * 0.1 }}
            >
              <span className={styles.featureIcon}>{feature.icon}</span>
              <h3 className={styles.featureTitle}>{feature.title}</h3>
              <p className={styles.featureDescription}>{feature.description}</p>
            </motion.div>
          ))}
        </div>
      </div>
    </section>
  );
}

// CTA Section
function CTASection() {
  return (
    <section className={styles.ctaSection}>
      <div className="container">
        <motion.div
          className={styles.ctaContent}
          initial={{ opacity: 0, scale: 0.95 }}
          whileInView={{ opacity: 1, scale: 1 }}
          viewport={{ once: true, margin: '-100px' }}
          transition={{ duration: 0.6 }}
        >
          <h2 className={styles.ctaTitle}>Ready to Process Events at Scale?</h2>
          <p className={styles.ctaSubtitle}>
            Join the community building the next generation of stream processing.
          </p>
          <div className={styles.ctaButtons}>
            <Link
              className="button button--primary button--lg"
              to="/docs/intro"
            >
              Read the Docs
            </Link>
            <Link
              className="button button--secondary button--lg"
              to="https://github.com/eventflux-io/eventflux/discussions"
            >
              Join the Discussion
            </Link>
          </div>
        </motion.div>
      </div>
    </section>
  );
}

// Main Homepage Component
export default function Home() {
  const { siteConfig } = useDocusaurusContext();

  return (
    <Layout
      title={`${siteConfig.title} - ${siteConfig.tagline}`}
      description="EventFlux is a high-performance Complex Event Processing (CEP) engine built in Rust. Process millions of events per second with SQL-like queries."
    >
      <HeroSection />
      <main>
        <FlowSection />
        <FeaturesSection />
        <CTASection />
      </main>
    </Layout>
  );
}
