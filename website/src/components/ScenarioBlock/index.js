import React, { useRef } from 'react';
import { motion, useInView } from 'framer-motion';
import CodeBlock from '@theme/CodeBlock';
import styles from './styles.module.css';

/**
 * ScenarioBlock animates in once when it scrolls into view.
 *
 * The scenario text slides and fades in from the left, then the code solution
 * slides in from the right. Set reversed to swap the sides.
 */
export default function ScenarioBlock({
  title,
  scenario,
  code,
  language = 'sql',
  icon,
  reversed = false,
}) {
  const ref = useRef(null);
  const isInView = useInView(ref, { once: true, margin: '-100px' });

  const containerVariants = {
    hidden: { opacity: 0 },
    visible: {
      opacity: 1,
      transition: {
        staggerChildren: 0.3,
        delayChildren: 0.1,
      },
    },
  };

  const scenarioVariants = {
    hidden: {
      opacity: 0,
      x: reversed ? 60 : -60,
    },
    visible: {
      opacity: 1,
      x: 0,
      transition: {
        duration: 0.6,
        ease: [0.25, 0.1, 0.25, 1],
      },
    },
  };

  const codeVariants = {
    hidden: {
      opacity: 0,
      x: reversed ? -60 : 60,
      scale: 0.95,
    },
    visible: {
      opacity: 1,
      x: 0,
      scale: 1,
      transition: {
        duration: 0.6,
        ease: [0.25, 0.1, 0.25, 1],
      },
    },
  };

  return (
    <motion.div
      ref={ref}
      className={`${styles.scenarioBlock} ${reversed ? styles.reversed : ''}`}
      variants={containerVariants}
      initial="hidden"
      animate={isInView ? 'visible' : 'hidden'}
    >
      <motion.div className={styles.scenarioContent} variants={scenarioVariants}>
        <div className={styles.iconWrapper}>
          {icon && <span className={styles.icon}>{icon}</span>}
        </div>
        <h3 className={styles.title}>{title}</h3>
        <p className={styles.scenario}>{scenario}</p>
      </motion.div>

      <motion.div className={styles.codeContent} variants={codeVariants}>
        <div className={styles.codeHeader}>
          <span className={styles.codeLabel}>Solution</span>
          <div className={styles.codeDots}>
            <span></span>
            <span></span>
            <span></span>
          </div>
        </div>
        <CodeBlock language={language} className={styles.codeBlock}>
          {code}
        </CodeBlock>
      </motion.div>
    </motion.div>
  );
}
