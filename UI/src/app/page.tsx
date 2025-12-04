import Navbar from "@/components/Navbar/Navbar";
import Hero from "@/components/Hero/Hero";
import styles from "./page.module.css";

export default function Home() {
  return (
    <main className={styles.main}>
      <Navbar />
      <Hero />

      <section className={styles.content}>
        <h2 className={styles.sectionTitle}>Browse by category</h2>
        <div className={styles.grid}>
          <div className={styles.card}>
            <div className={styles.cardImage} style={{ backgroundColor: '#e0f2fe' }}></div>
            <h3>New homes</h3>
            <p>See the latest new builds on the market</p>
          </div>
          <div className={styles.card}>
            <div className={styles.cardImage} style={{ backgroundColor: '#fce7f3' }}></div>
            <h3>Commercial</h3>
            <p>Find commercial property to rent or buy</p>
          </div>
          <div className={styles.card}>
            <div className={styles.cardImage} style={{ backgroundColor: '#dcfce7' }}></div>
            <h3>Overseas</h3>
            <p>Find your dream home abroad</p>
          </div>
        </div>
      </section>
    </main>
  );
}
